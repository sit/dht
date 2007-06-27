#include <async.h>
#include <arpc.h>
#include <wmstr.h>
#include <ihash.h>
#include <sha1.h>

#include <adb_prot.h>
#include <id_utils.h>
#include <dbfe.h>
#include <merkle_tree_bdb.h>

// {{{ Globals
static bool dbstarted (false);
static str dbsock;
static int clntfd (-1);

static const u_int32_t asrvbufsize (1024*1025);

class dbmanager;
static dbmanager *dbm;
// }}}
// {{{ IO timing
static bool iotime (false);

static inline u_int64_t
io_start ()
{
  if (!iotime)
    return 0;
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  u_int64_t key = ts.tv_sec * INT64(1000000) + ts.tv_nsec / 1000;
  return key;
}

static inline void
io_finish (u_int64_t t, strbuf s)
{
  if (!iotime)
    return;
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  u_int64_t now = ts.tv_sec * INT64(1000000) + ts.tv_nsec / 1000;
  warn << "IO: " << s << " in " << (now - t) << "us\n";
}

static void
toggle_iotime ()
{
  iotime = !iotime;
}
// }}}
// {{{ DB key conversion
inline void
id_to_dbt (const chordID &key, DBT *d)
{
  static char buf[sha1::hashsize]; // XXX bug waiting to happen?

  bzero (d, sizeof (*d));
  bzero (buf, sizeof (buf)); // XXX unnecessary; handled by rawmag.

  // We want big-endian for BTree mapping efficiency
  mpz_get_rawmag_be (buf, sizeof (buf), &key);
  d->size = sizeof (buf);
  d->data = (void *) buf;
}

inline void
str_to_dbt (const str &s, DBT *d)
{
  bzero (d, sizeof (*d));
  d->size = s.len ();
  d->data = (void *) s.cstr ();
}

inline chordID
dbt_to_id (const DBT &dbt)
{
  chordID id;
  assert (dbt.size == sha1::hashsize);
  mpz_set_rawmag_be (&id, static_cast<char *> (dbt.data), dbt.size);
  return id;
}
// }}}

// {{{ DB for Namespace
/* For each namespace (e.g. vnode + ctype),
 * we need to store several things:
 *   1. chordID -> data.  20 bytes -> potentially large bytes
 *   2. chordID -> metadata: 20 bytes -> ...
 *        auxdata (e.g. noauth hash) 4 + 4*n bytes
 *
 * The goal is to support the following operations:
 *   A. Read/write of data objects
 *   B. Merkle tree updates (shared with maintd via BDB)
 */ 

// {{{ dbns declarations
class dbns {
  friend class dbmanager;

  str name;
  ihash_entry<dbns> hlink;

  DB_ENV *dbe;

  DB *datadb;
  DB *auxdatadb;

  merkle_tree *mtree;

  // XXX should have a instance-level lock that governs datadb/auxdatadb.

public:
  dbns (const str &dbpath, const str &name, bool aux);
  ~dbns ();

  void warner (const char *method, const char *desc, int r);

  void sync (bool force = false);

  bool hasaux () { return auxdatadb != NULL; };

  // Primary data management
  bool kinsert (const chordID &key, u_int32_t auxdata);
  int insert (const chordID &key, DBT &data, DBT &auxdata);
  int lookup (const chordID &key, str &data);
  int lookup_nextkey (const chordID &key, chordID &nextkey);
  int del (const chordID &key, u_int32_t auxdata);
  int getkeys (const chordID &start, size_t count, bool getaux,
      rpc_vec<adb_keyaux_t, RPC_INFINITY> &out); 
};
// }}}
// {{{ dbns::dbns
dbns::dbns (const str &dbpath, const str &name, bool aux) :
  name (name),
  dbe (NULL),
  datadb (NULL),
  auxdatadb (NULL)
{
#define DBNS_ERRCHECK(desc) \
  if (r) {		  \
    fatal << desc << " returned " << r << ": " << db_strerror (r) << "\n"; \
    return;		  \
  }
  assert (dbpath[dbpath.len () - 1] == '/');
  strbuf fullpath ("%s%s", dbpath.cstr (), name.cstr ());

  int r = -1;
  r = dbfe_initialize_dbenv (&dbe, fullpath, false, 30*1024);
  DBNS_ERRCHECK ("dbe->open");

  r = dbfe_opendb (dbe, &datadb, "db", DB_CREATE, 0);
  DBNS_ERRCHECK ("datadb->open");
  if (aux) {
    r = dbfe_opendb (dbe, &auxdatadb, "auxdb", DB_CREATE, 0);
    DBNS_ERRCHECK ("auxdatadb->open");
  }

  mtree = New merkle_tree_bdb (dbe, /* ro = */ false);

  warn << "dbns::dbns (" << dbpath << ", " << name << ", " << aux << ")\n";
}
// }}}
// {{{ dbns::~dbns
dbns::~dbns ()
{
  sync (/* force = */ true);

#define DBNS_DBCLOSE(x)			\
  if (x) {				\
    (void) x->close (x, 0); x = NULL;	\
  }

  // Start with main databases
  DBNS_DBCLOSE(datadb);
  DBNS_DBCLOSE(auxdatadb);
  // Close out the merkle tree which shares our db environment
  delete mtree;
  mtree = NULL;
  // Shut down the environment
  DBNS_DBCLOSE(dbe);
#undef DBNS_DBCLOSE
  warn << "dbns::~dbns (" << name << ")\n";
}
// }}}
// {{{ dbns::warner
void
dbns::warner (const char *method, const char *desc, int r)
{
  timespec ts;
  strbuf t;
  clock_gettime (CLOCK_REALTIME, &ts);
  t.fmt ("%d.%06d ", int (ts.tv_sec), int (ts.tv_nsec/1000));
  warn << t << ": " << method << ": " << desc << ": " << db_strerror (r) << "\n";
}
// }}}
// {{{ dbns::sync
void
dbns::sync (bool force)
{
  int flags = 0;
  if (force)
    flags = DB_FORCE;
#if (DB_VERSION_MAJOR < 4)
  txn_checkpoint (dbe, 30*1024, 10, flags);
#else
  dbe->txn_checkpoint (dbe, 30*1024, 10, flags);
#endif
}
// }}}
// {{{ dbns::kinsert (chordID, u_int32_t)
bool
dbns::kinsert (const chordID &key, u_int32_t auxdata)
{
  // We don't really deal well with updating these auxiliary
  // databases yet, so just ignore them for now.  The auxdatadb
  // should iterate much faster than the full database anyway.
  // if (!auxdatadb)
  if (hasaux ()) {
    mtree->insert (key, auxdata);
  } else {
    if (mtree->key_exists (key)) {
      return false;
    }
    mtree->insert (key);
  }
  return true;
}
// }}}
// {{{ dbns::insert (chordID, DBT, DBT)
int
dbns::insert (const chordID &key, DBT &data, DBT &auxdata)
{
  int r = 0;
  DBT skey;
  id_to_dbt (key, &skey);

  if (auxdatadb) {
    // To keep auxdata in sync, use an explicit transaction
    DB_TXN *t = NULL;
    r = dbfe_txn_begin (dbe, &t);
    r = datadb->put (datadb, t, &skey, &data, 0);
    if (r) {
      warner ("dbns::insert", "data put error", r);
      r = dbfe_txn_abort (dbe, t);
    } 
    r = auxdatadb->put (auxdatadb, t, &skey, &auxdata, 0);
    if (r) {
      warner ("dbns::insert", "auxdata put error", r);
      r = dbfe_txn_abort (dbe, t);
    } else {
      r = dbfe_txn_commit (dbe, t);
    }
    if (r)
      warner ("dbns::insert", "abort/commit error", r);
  } else {
    // Use implicit transaction
    r = datadb->put (datadb, NULL, &skey, &data, DB_AUTO_COMMIT);
    if (r)
      warner ("dbns::insert", "put error", r);
  }
  return r;
}
// }}}
// {{{ dbns::lookup
int
dbns::lookup (const chordID &key, str &data)
{
  int r = 0;

  DBT skey;
  id_to_dbt (key, &skey);

  DBT content;
  bzero (&content, sizeof (content));

  // Implicit transaction
  r = datadb->get (datadb, NULL, &skey, &content, 0);
  if (r) {
    if (r != DB_NOTFOUND)
      warner ("dbns::fetch", "get error", r);
    return r;
  }
  data.setbuf ((const char *) (content.data), content.size);
  return 0;
}
// }}}
// {{{ dbns::lookup_next
int
dbns::lookup_nextkey (const chordID &key, chordID &nextkey)
{
  int r = 0;

  DBT skey;
  id_to_dbt (key, &skey);

  DBT content;
  bzero (&content, sizeof (content));

  DBC *cursor;
  r = datadb->cursor (datadb, NULL, &cursor, 0);

  if (r) {
    warner ("dbns::lookup_nextkey", "cursor open", r);
    (void) cursor->c_close (cursor);
    return r;
  }

  // Implicit transaction
  r = cursor->c_get (cursor, &skey, &content, DB_SET_RANGE);
  // Loop around the ring if at end.
  if (r == DB_NOTFOUND) {
    bzero (&skey, sizeof (skey));
    r = cursor->c_get (cursor, &skey, &content, DB_FIRST);
  }

  if (r) {
    if (r != DB_NOTFOUND)
      warner ("dbns::fetch", "get error", r);
    (void) cursor->c_close (cursor);
    return r;
  }
  nextkey = dbt_to_id(skey);
  (void) cursor->c_close (cursor);
  return 0;
}
// }}}
// {{{ dbns::del
int
dbns::del (const chordID &key, u_int32_t auxdata)
{
  int r = 0;
  DBT skey;
  id_to_dbt (key, &skey);

  // Implicit transaction
  r = datadb->del (datadb, NULL, &skey, DB_AUTO_COMMIT);

  if (hasaux ()) {
    mtree->remove (key, auxdata);
  } else {
    mtree->remove (key);
  }

  if (r && r != DB_NOTFOUND)
    warner ("dbns::del", "del error", r);
  return r;
}
// }}}
// {{{ dbns::getkeys
int
dbns::getkeys (const chordID &start, size_t count, bool getaux, rpc_vec<adb_keyaux_t, RPC_INFINITY> &out)
{
  int r = 0;
  DBC *cursor;
  if (auxdatadb) {
    r = auxdatadb->cursor (auxdatadb, NULL, &cursor, 0);
  } else {
    r = datadb->cursor (datadb, NULL, &cursor, 0);
  }

  if (r) {
    warner ("dbns::getkeys", "cursor open", r);
    (void) cursor->c_close (cursor);
    return r;
  }

  // Could possibly improve efficiency here by using SleepyCat's bulk reads
  DBT key;
  id_to_dbt (start, &key);
  DBT data_template;
  bzero (&data_template, sizeof (data_template));
  // If DB_DBT_PARTIAL and data.dlen == 0, no data is read.
  if (!auxdatadb || !getaux)
    data_template.flags = DB_DBT_PARTIAL;
  DBT data = data_template;

  u_int32_t limit = count;
  if (count < 0)
    limit = (u_int32_t) (asrvbufsize/(1.5*sizeof (adb_keyaux_t)));

  // since we set a limit, we know the maximum amount we have to allocate
  out.setsize (limit);
  u_int32_t elements = 0;

  r = cursor->c_get (cursor, &key, &data, DB_SET_RANGE);
  while (elements < limit && !r) {
    out[elements].key = dbt_to_id (key);
    if (getaux)
      out[elements].auxdata = ntohl (*(u_int32_t *)data.data);
    elements++;

    bzero (&key, sizeof (key));
    data = data_template;
    r = cursor->c_get (cursor, &key, &data, DB_NEXT);
  }

  if (elements < limit) {
    out.setsize (elements);
  }

  if (r && r != DB_NOTFOUND)
    warner ("dbns::getkeys", "cursor get", r);
  (void) cursor->c_close (cursor);

  return r;
}
// }}}
// }}}

// {{{ DB Manager
class dbmanager {
  str dbpath;
  ihash<str, dbns, &dbns::name, &dbns::hlink> dbs;
  void dbcloser (dbns *db);
public:
  dbmanager (str);
  ~dbmanager ();
  const str &getdbpath () { return dbpath; };

  dbns *get (const str &n) { return dbs[n]; };
  dbns *createdb (const str &n, bool aux);
};

dbmanager::dbmanager (str p) :
  dbpath (p)
{
  // Make path if necessary
  struct stat sb;
  if (stat (dbpath, &sb) < 0) {
    if (errno == ENOENT) {
      if (mkdir (dbpath, 0755) < 0)
	fatal ("dbmanager::dbmanager: mkdir (%s): %m\n", dbpath.cstr ());
    } else {
      fatal ("dbmanager::dbmanager: stat (%s): %m\n", dbpath.cstr ());
    }
  } else {
    if (!S_ISDIR (sb.st_mode)) 
      fatal ("dbmanager::dbmanager: %s is not a directory\n", dbpath.cstr ());
    if (access (dbpath, W_OK) < 0)
      fatal ("dbmanager::manager: access (%s, W_OK): %m\n", dbpath.cstr ());
  }

  // Convert path to full path
  char realpath[MAXPATHLEN];
  int fd = -1;
  if (((fd = open (".", O_RDONLY)) >= 0)
     && chdir (dbpath) >= 0)
  {
    if (getcwd (realpath, sizeof (realpath)))
      errno = 0;
    else if (!errno)
      errno = EINVAL;
    if (fchdir (fd))
      warn ("fchdir: %m\n");
  }

  // Add trailing slash if necessary
  str x (realpath);
  if (x[x.len () - 1] != '/')
    dbpath = strbuf () << x << "/";
  else 
    dbpath = x;
}

dbmanager::~dbmanager ()
{
  dbs.traverse (wrap (this, &dbmanager::dbcloser));
  dbs.clear ();
}

void
dbmanager::dbcloser (dbns *db)
{
  if (db) {
    warn << "Closing db " << db->name << "\n";
    delete db;
    db = NULL;
  }
}

dbns *
dbmanager::createdb (const str &n, bool aux)
{
  dbns *db = dbs[n];
  if (!db) {
    db = New dbns (dbpath, n, aux);
    dbs.insert (db);
  }
  return db;
}
// }}}

// {{{ Shutdown functions
EXITFN (cleanup);
static void
cleanup ()
{
  if (dbstarted) {
    if (clntfd >= 0) {
      fdcb (clntfd, selread, NULL);
      close (clntfd);
    }
    unlink (dbsock);
  }
}

static void
halt ()
{
  warn << "Exiting on command...\n";
  delete dbm;
  exit (0);
}
// }}}

// {{{ RPC execution
// {{{ do_initspace
void
do_initspace (dbmanager *dbm, svccb *sbp)
{
  adb_initspacearg *arg = sbp->Xtmpl getarg<adb_initspacearg> ();
  adb_status stat (ADB_OK);
  dbns *db = dbm->get (arg->name);
  if (db) {
    sbp->replyref (stat); 
    return;
  }
  db = dbm->createdb (arg->name, arg->hasaux);
  stat = (db ? ADB_OK : ADB_ERR);
  sbp->replyref (stat);
}
// }}}
// {{{ do_store
void
do_store (dbmanager *dbm, svccb *sbp)
{

  adb_storearg *arg = sbp->Xtmpl getarg<adb_storearg> ();
  dbns *db = dbm->get (arg->name);
  if (!db) {
    sbp->replyref (ADB_ERR);
    return;
  }

  u_int64_t t = io_start ();

  // implicit policy: don't insert the same key twice for non-aux dbs.
  bool inserted_new_key = db->kinsert (arg->key, arg->auxdata);
  if (!inserted_new_key) {
    io_finish (t, strbuf("store %s", arg->name.cstr ()));
    sbp->replyref (ADB_OK);
    return;
  }

  DBT data;
  bzero (&data, sizeof (data));
  data.data = arg->data.base ();
  data.size = arg->data.size ();

  DBT auxdatadbt;
  bzero (&auxdatadbt, sizeof (auxdatadbt));
  auxdatadbt.size = sizeof (u_int32_t);
  u_int32_t nauxdata = htonl (arg->auxdata);
  auxdatadbt.data = &nauxdata; 

  int r = db->insert (arg->key, data, auxdatadbt);
  io_finish (t, strbuf("store %s", arg->name.cstr ()));
  sbp->replyref ((r == 0) ? ADB_OK : ADB_ERR);
}
// }}}
// {{{ do_fetch
void
do_fetch (dbmanager *dbm, svccb *sbp)
{
  adb_fetcharg *arg = sbp->Xtmpl getarg<adb_fetcharg> ();
  adb_fetchres res (ADB_OK); 

  dbns *db = dbm->get (arg->name);
  if (!db) {
    res.set_status (ADB_ERR);
    sbp->replyref (res);
    return;
  }

  u_int64_t t = io_start ();
 
  str data; 

  int r;
  chordID key;
  if (arg->nextkey) {
    r = db->lookup_nextkey (arg->key, key);
    data = "";
  } else {
    r = db->lookup (arg->key, data);
    key = arg->key;
  }

  if (r) {
    res.set_status ((r == DB_NOTFOUND ? ADB_NOTFOUND : ADB_ERR));
  } else {
    res.resok->key = key;
    res.resok->data = data;
  }

  io_finish (t, strbuf ("fetch %s", arg->name.cstr ()));

  sbp->replyref (res);
}
// }}}
// {{{ do_getkeys
void
do_getkeys (dbmanager *dbm, svccb *sbp)
{
  adb_getkeysarg *arg = sbp->Xtmpl getarg<adb_getkeysarg> ();
  adb_getkeysres res (ADB_OK);

  dbns *db = dbm->get (arg->name);
  if (!db) {
    res.set_status (ADB_ERR);
    sbp->replyref (res);
    return;
  }
  res.resok->hasaux = db->hasaux () && arg->getaux;
  res.resok->ordered = arg->ordered;

  int r (-1);
  r = db->getkeys (arg->continuation, arg->batchsize, arg->getaux, res.resok->keyaux);
  if (!r)
    res.resok->continuation = incID (res.resok->keyaux.back ().key);
  res.resok->complete = (r == DB_NOTFOUND);
  if (r && r != DB_NOTFOUND) 
    res.set_status (ADB_ERR);
  sbp->replyref (res);
}
// }}}
// {{{ do_delete
void
do_delete (dbmanager *dbm, svccb *sbp)
{
  adb_deletearg *arg = sbp->Xtmpl getarg<adb_deletearg> ();
  dbns *db = dbm->get (arg->name);
  if (!db) {
    sbp->replyref (ADB_ERR);
    return;
  }
  int r = db->del (arg->key, arg->auxdata);
  sbp->replyref ((r == 0) ? ADB_OK : ADB_NOTFOUND);
}

// }}}
// {{{ do_getspaceinfo
void
do_getspaceinfo (dbmanager *dbm, svccb *sbp)
{
  adb_dbnamearg *arg = sbp->Xtmpl getarg<adb_dbnamearg> ();
  adb_getspaceinfores *res = sbp->Xtmpl getres<adb_getspaceinfores> ();
  res->status = ADB_OK;
  dbns *db = dbm->get (arg->name);
  if (!db) {
    sbp->replyref (ADB_ERR);
    return;
  }

  res->fullpath = strbuf () << dbm->getdbpath () << arg->name;
  res->hasaux = db->hasaux ();
  sbp->reply (res);
}
// }}}
// {{{ do_sync
void
do_sync (dbmanager *dbm, svccb *sbp)
{
  adb_dbnamearg *arg = sbp->Xtmpl getarg<adb_dbnamearg> ();
  adb_status res (ADB_OK);
  dbns *db = dbm->get (arg->name);
  if (!db) {
    res = ADB_ERR;
  } else {
    u_int64_t t = io_start ();
    db->sync ();
    io_finish (t, strbuf ("sync %s", arg->name.cstr ()));
  }
  sbp->replyref (res);
}
// }}}
// }}}

// {{{ RPC accept and dispatch
void
dispatch (ref<axprt_stream> s, ptr<asrv> a, dbmanager *dbm, svccb *sbp)
{
  if (sbp == NULL) {
    warn << "EOF from client\n";
    a = NULL;
    return;
  }

  switch (sbp->proc ()) {
  case ADBPROC_INITSPACE:
    do_initspace (dbm, sbp);
    break;
  case ADBPROC_STORE:
    do_store (dbm, sbp);
    break;
  case ADBPROC_FETCH:
    do_fetch (dbm, sbp);
    break;
  case ADBPROC_GETKEYS:
    do_getkeys (dbm, sbp);
    break;
  case ADBPROC_DELETE:
    do_delete (dbm, sbp);
    break;
  case ADBPROC_GETSPACEINFO:
    do_getspaceinfo (dbm, sbp);
    break;
  case ADBPROC_SYNC:
    do_sync (dbm, sbp);
    break;
  default:
    fatal << "unknown procedure: " << sbp->proc () << "\n";
  }
  
  return;
}

static void accept_cb (int lfd, dbmanager *dbm);
static void
listen_unix (str sock_name, dbmanager *dbm)
{
  unlink (sock_name);
  clntfd = unixsocket (sock_name);
  if (clntfd < 0) 
    fatal << "Error creating socket (UNIX)" << strerror (errno) << "\n";
  
  if (listen (clntfd, 128) < 0) {
    fatal ("Error from listen: %m\n");
    close (clntfd);
  } else {
    fdcb (clntfd, selread, wrap (accept_cb, clntfd, dbm));
  }
  dbstarted = true;
}

static void 
accept_cb (int lfd, dbmanager *dbm)
{
  sockaddr_un sun;
  bzero (&sun, sizeof (sun));
  socklen_t sunlen = sizeof (sun);
  int fd = accept (lfd, reinterpret_cast<sockaddr *> (&sun), &sunlen);
  if (fd < 0)
    fatal ("EOF\n");

  ref<axprt_stream> x = axprt_stream::alloc (fd, asrvbufsize);

  ptr<asrv> a = asrv::alloc (x, adb_program_1);
  a->setcb (wrap (dispatch, x, a, dbm));
}
// }}}

void
usage ()
{
  warnx << "Usage: adbd -d db -S sock [-D]\n";
  exit (0);
}

int 
main (int argc, char **argv)
{

  setprogname (argv[0]);
  mp_set_memory_functions (NULL, simple_realloc, NULL);

  char ch;
  str db_name = "/var/tmp/db";
  dbsock = "/tmp/db-sock";

  bool do_daemonize (false);

  while ((ch = getopt (argc, argv, "Dd:S:"))!=-1)
    switch (ch) {
    case 'D':
      do_daemonize = true;
      break;
    case 'd':
      db_name = optarg;
      break;
    case 'S':
      dbsock = optarg;
      break;
    default:
      usage ();
    }
 
  if (do_daemonize) {
    warn << "starting daemonized\n";
    daemonize ();
  }
  warn << "db path: " << db_name << "\n";
  warn << "db socket: " << dbsock << "\n";

  dbm = New dbmanager(db_name);

  sigcb (SIGHUP, wrap (&halt));
  sigcb (SIGINT, wrap (&halt));
  sigcb (SIGTERM, wrap (&halt));

  sigcb (SIGUSR1, wrap (&toggle_iotime));

  //setup the asrv
  listen_unix (dbsock, dbm);

  amain ();
}

// vim: foldmethod=marker
