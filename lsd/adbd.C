#include <async.h>
#include <arpc.h>
#include <wmstr.h>
#include <ihash.h>
#include <sha1.h>
#include <parseopt.h>

#include <adb_prot.h>
#include <id_utils.h>
#include <dbfe.h>
#include <merkle_tree_bdb.h>

// {{{ Globals
static bool dbstarted (false);
static str dbsock;
static int clntfd (-1);

static const u_int32_t asrvbufsize (1024*1025);

// Total disk allowed for data per dbns, in bytes.
// Default: unlimited
static u_int64_t quota (0);

// How many keys to try and expire at a time between stores.
static u_int32_t expire_batch_size (24);

// Threshold percentage for expiring on insert
static u_int32_t expire_threshold (90);

// This is the key used to access the master metadata record.
// The master metadata record contains:
//   Total size of objects put into a dbns,
//   The next expiration time for an object.
static DBT master_metadata;

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
 *        expiration time
 *        size
 *
 * The goal is to support the following operations:
 *   A. Read/write of data objects
 *   B. Merkle tree updates (shared with maintd via BDB)
 */ 

// {{{ getexpire: Secondary key extractor for metadata
static int
getexpire (DB *sdb, const DBT *pkey, const DBT *pdata, DBT *skey)
{
  u_int32_t *expire = (u_int32_t *) malloc(sizeof(u_int32_t));

  adb_metadata_t md;
  if (!buf2xdr (md, pdata->data, pdata->size)) {
    hexdump hd (pdata->data, pdata->size);
    warn << "getexpire: unable to unmarshal pdata.\n" << hd << "\n";
    return -1;
  }
  // Ensure big-endian for proper BDB sorting.
  *expire = htonl (md.expiration);
  // this flag should mean that berkeley db will free this memory when it's
  // done with it.
  skey->flags = DB_DBT_APPMALLOC;
  skey->data = expire;
  skey->size = sizeof (*expire);
  return 0;
}
// }}}
// {{{ dbns declarations
class dbns {
  friend class dbmanager;

  str name;
  ihash_entry<dbns> hlink;

  const bool aux;
  DB_ENV *dbe;

  DB *datadb;
  DB *metadatadb;
  DB *byexpiredb;

  merkle_tree_bdb *mtree;

  adb_master_metadata_t mmd;

  int update_metadata (int32_t sz, u_int32_t expiration, DB_TXN *t = NULL);

public:
  dbns (const str &dbpath, const str &name, bool aux, str logpath = NULL);
  ~dbns ();

  void warner (const char *method, const char *desc, int r);

  void sync (bool force = false);

  bool hasaux () { return aux; };
  str getname () { return name; }

  // Primary data management
  int insert (const chordID &key, DBT &data, u_int32_t auxdata = 0, u_int32_t exptime = 0);
  int lookup (const chordID &key, str &data);
  int lookup_nextkey (const chordID &key, chordID &nextkey);
  int del (const chordID &key, u_int32_t auxdata);
  int getkeys (const chordID &start, size_t count, bool getaux,
      rpc_vec<adb_keyaux_t, RPC_INFINITY> &out); 

  u_int32_t quotacheck (u_int64_t q) {
    // Returns percentage between 0 and 100
    if (q == 0) return 0;
    if (q < mmd.size) return 100;
    return u_int64_t (100) * mmd.size / q;
  }
  int expire (u_int32_t limit = 0, u_int32_t t = 0);
};
// }}}
// {{{ dbns::dbns
dbns::dbns (const str &dbpath, const str &name, bool aux, str logpath) :
  name (name),
  aux (aux),
  dbe (NULL),
  datadb (NULL),
  metadatadb (NULL),
  byexpiredb (NULL),
  mtree (NULL)
{
  bzero (&mmd, sizeof (mmd));
#define DBNS_ERRCHECK(desc) \
  if (r) {		  \
    fatal << desc << " returned " << r << ": " << db_strerror (r) << "\n"; \
    return;		  \
  }
  assert (dbpath[dbpath.len () - 1] == '/');
  strbuf fullpath ("%s%s", dbpath.cstr (), name.cstr ());

  str logconf = NULL;
  if (logpath)
    logconf = strbuf ("set_lg_dir %s/%s\n", logpath.cstr (), name.cstr ());

  int r = -1;
  r = dbfe_initialize_dbenv (&dbe, fullpath, false, 30*1024, logconf);
  DBNS_ERRCHECK ("dbe->open");

  r = dbfe_opendb (dbe, &datadb, "db", DB_CREATE, 0);
  DBNS_ERRCHECK ("datadb->open");

  mtree = New merkle_tree_bdb (dbe, /* ro = */ false);

  r = dbfe_opendb (dbe, &metadatadb, "metadatadb", DB_CREATE, 0);
  DBNS_ERRCHECK ("metadatadb->open");
  r = dbfe_opendb (dbe, &byexpiredb, "byexpiredb", DB_CREATE, 0, /* dups = */ true);
  DBNS_ERRCHECK ("byexpiredb->open");
  r = metadatadb->associate (metadatadb, NULL, byexpiredb, getexpire, DB_AUTO_COMMIT);
  DBNS_ERRCHECK ("metadatdb->associate (byexpiredb)");

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
  // Close out the merkle tree which shares our db environment
  delete mtree;
  mtree = NULL;
  // Close secondary before the primary for metadata
  DBNS_DBCLOSE(byexpiredb);
  DBNS_DBCLOSE(metadatadb);
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
// {{{ dbns::update_metadata (int32_t, u_int32_t, DB_TXN *)
int
dbns::update_metadata (int32_t sz, u_int32_t expiration, DB_TXN *t)
{
  DBT md; bzero (&md, sizeof (md));
  int r = metadatadb->get (metadatadb, t, &master_metadata, &md, DB_RMW);
  switch (r) {
    case 0:
      if (!buf2xdr (mmd, md.data, md.size))
	return -1;
      break;
    case DB_NOTFOUND:
      bzero (&mmd, sizeof (mmd));
      break;
    default:
      return r;
      break;
  }
  if (sz < 0 && mmd.size < u_int64_t (-sz)) {
    fatal << "dbns::update_metadata: small size: "
          << mmd.size << " < " << sz << "\n";
  }
  mmd.size += sz;
  if (quota && mmd.size > quota)
    return ENOSPC;
  if (mmd.expiration == 0 && expiration > 0) {
    if (sz > 0 && expiration < mmd.expiration)
      mmd.expiration = expiration;
    else if (sz < 0) {
      assert (expiration >= mmd.expiration);
      mmd.expiration = expiration;
    }
  }

  str md_str = xdr2str (mmd);
  str_to_dbt (md_str, &md);
  r = metadatadb->put (metadatadb, t, &master_metadata, &md, 0);
  return r;
}
// }}}
// {{{ dbns::insert (chordID, DBT, DBT)
int
dbns::insert (const chordID &key, DBT &data, u_int32_t auxdata, u_int32_t exptime)
{
  int r = 0;
  DB_TXN *t = NULL;
  r = dbfe_txn_begin (dbe, &t);
  assert (r == 0);

  r = update_metadata (data.size, exptime, t);
  if (r) {
    dbfe_txn_abort (dbe, t);
    // Even if r == ENOSPC, we can't afford to blow a lot of time
    // here doing expiration.
    return r;
  }

  // Prep BDB objects.
  DBT skey;
  id_to_dbt (key, &skey);

  adb_metadata_t md;
  md.size = data.size;
  md.auxdata = auxdata;
  md.expiration = exptime;
  str md_str = xdr2str (md);
  DBT metadata;
  str_to_dbt (md_str, &metadata);

  char *err = "";
  do {
    if (hasaux ()) {
      err = "mtree->insert aux";
      r = mtree->insert (key, auxdata, t);
      if (r) break;
    } else {
      err = "mtree->insert";
      r = mtree->insert (key, t);
      // insert may return DB_KEYEXIST in which case we need
      // not do any more work here.
      if (r) break;
    }
    err = "metadatadb->put";
    r = metadatadb->put (metadatadb, t, &skey, &metadata, 0);
    if (r) break;

    err = "datadb->put";
    r = datadb->put (datadb, t, &skey, &data, 0);
    if (r) break;
  } while (0);
  int ret = 0;
  if (r) {
    if (r != DB_KEYEXIST)
      warner ("dbns::insert", err, r);
    ret = dbfe_txn_abort (dbe, t);
  } else {
    ret = dbfe_txn_commit (dbe, t);
  }
  if (ret)
    warner ("dbns::insert", "abort/commit error", ret);
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

  // Read the old size in separate transaction
  DBT md;
  bzero (&md, sizeof (md));
  r = metadatadb->get (metadatadb, NULL, &skey, &md, 0);
  if (r) {
    if (r != DB_NOTFOUND)
      warner ("dbns::remove", "metadatadb->get", r);
    return r;
  }
  adb_metadata_t metadata;
  if (!buf2xdr (metadata, md.data, md.size))
    return -1;

  DB_TXN *t = NULL;
  r = dbfe_txn_begin (dbe, &t);
  assert (r == 0);

  // Update min expiration lazily; we can't know if this is the
  // last object with a particular expiration time.
  r = update_metadata (-metadata.size, 0, t);
  if (r) {
    dbfe_txn_abort (dbe, t);
    return r;
  }

  char *err = "";
  do {
    if (hasaux ()) {
      err = "mtree->remove aux";
      r = mtree->remove (key, auxdata, t);
      if (r) break;
    } else {
      err = "mtree->remove";
      r = mtree->remove (key, t);
      if (r) break;
    }
    err = "metadatadb->del";
    r = metadatadb->del (metadatadb, t, &skey, 0);
    if (r) break;

    err = "datadb->del";
    r = datadb->del (datadb, t, &skey, 0);
    if (r) break;
  } while (0);

  int ret = 0;
  if (r) {
    if (r != DB_NOTFOUND)
      warner ("dbns::remove", err, r);
    ret = dbfe_txn_abort (dbe, t);
  } else {
    ret = dbfe_txn_commit (dbe, t);
  }
  if (ret)
    warner ("dbns::remove", "abort/commit error", ret);
  return r;
}
// }}}
// {{{ dbns::getkeys
int
dbns::getkeys (const chordID &start, size_t count, bool getaux, rpc_vec<adb_keyaux_t, RPC_INFINITY> &out)
{
  int r = 0;
  DBC *cursor;
  r = metadatadb->cursor (metadatadb, NULL, &cursor, 0);
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
  DBT data = data_template;

  u_int32_t limit = count;
  if (count < 0)
    limit = (u_int32_t) (asrvbufsize/(1.5*sizeof (adb_keyaux_t)));

  // since we set a limit, we know the maximum amount we have to allocate
  out.setsize (limit);
  u_int32_t elements = 0;

  r = cursor->c_get (cursor, &key, &data, DB_SET_RANGE);
  while (elements < limit && !r) {
    if (key.size == master_metadata.size && 
	!memcmp (key.data, master_metadata.data, key.size))
      goto getkeys_nextkey;
    out[elements].key = dbt_to_id (key);
    if (getaux) {
      adb_metadata_t md;
      if (!buf2xdr (md, data.data, data.size)) {
	warnx << name << ": Bad metadata for " << out[elements].key << "\n";
	goto getkeys_nextkey;
      }
      out[elements].auxdata = md.auxdata;
    }
    elements++;

getkeys_nextkey:
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
// {{{ dbns::expire (u_int32_t, u_int32_t)
int
dbns::expire (u_int32_t limit, u_int32_t deadline)
{
  if (deadline == 0)
    deadline = time (NULL);

  u_int64_t victim_size = 0;
  u_int32_t last_expire = 0;
  vec<DBT> victims;
  vec<adb_metadata_t> victim_metadata;

  // XXX Would it be okay to modify the database (and its
  //     sibling databases) while the cursor is open?

  // Open a cursor in secondary database.
  // Make sure it points to the first thing after 0.
  // Keep reading and deleting until the thing's key is > t.
  //   Accumulate entries into a vec, including the object size.
  u_int32_t begin_time_data = htonl (1);
  DBT begin_time; bzero (&begin_time, sizeof (begin_time));
  begin_time.data = &begin_time_data;
  begin_time.size = sizeof (begin_time_data);
  DBT key; bzero (&key, sizeof (key));
  key.flags = DB_DBT_MALLOC;
  DBT content; bzero (&content, sizeof (content));
  DBC *cursor = NULL;
  int r = byexpiredb->cursor (byexpiredb, NULL, &cursor, 0);
  if (r) {
    warner ("dbns::expire", "byexpiredb->cursor", r);
    if (cursor)
      cursor->c_close (cursor);
    return r;
  }
  r = cursor->c_pget (cursor, &begin_time, &key, &content, DB_SET_RANGE);
  while (!r && (limit == 0 || victims.size () < limit)) {
    if (key.size == master_metadata.size && 
	!memcmp (key.data, master_metadata.data, key.size))
    {
      free (key.data);
      goto expire_nextkey;
    }
    adb_metadata_t md;
    buf2xdr (md, content.data, content.size);
    if (md.expiration < deadline) {
      victims.push_back (key);
      victim_metadata.push_back (md);
    } else {
      free (key.data);
      break;
    }
expire_nextkey:
    bzero (&key, sizeof (key));
    key.flags = DB_DBT_MALLOC;
    r = cursor->c_pget (cursor, &begin_time, &key, &content, DB_NEXT);
  }
  if (r && r != DB_NOTFOUND)
    warner ("dbns::expire", "byexpiredb cursor->c_pget", r);
  (void) cursor->c_close (cursor);

  // start a transaction
  DB_TXN *parent = NULL;
  dbfe_txn_begin (dbe, &parent);
  // Iterate over objects to be expired:
  while (victims.size ()) {
    DB_TXN *t = NULL;
    dbe->txn_begin (dbe, parent, &t, 0);
    DBT key = victims.pop_back ();
    adb_metadata_t md = victim_metadata.pop_back ();
    chordID id = dbt_to_id (key);
    warnx << name << ": Expiring " << id << "\n";
    char *err = "";
    do {
      err = "mtree->remove";
      if (hasaux ()) {
	r = mtree->remove (id, md.auxdata, t);
      } else {
	r = mtree->remove (id, t);
      }
      if (r) break;
      err = "metadatadb->del";
      r = metadatadb->del (metadatadb, t, &key, 0);
      if (r) break;
      err = "datadb->del";
      r = datadb->del (datadb, t, &key, 0);
      if (r) break;
    } while (0);
    free (key.data);
    if (r) {
      warner ("dbns::expire", err, r);
      dbfe_txn_abort (dbe, t);
    } else {
      victim_size += md.size;
      if (md.expiration > last_expire)
	last_expire = md.expiration;
      dbfe_txn_commit (dbe, t);
    }
  }
  // Update the metadata with size/time difference
  r = update_metadata (-victim_size, last_expire, parent);
  dbfe_txn_commit (dbe, parent);
  return r;
}
// }}}
// }}}

// {{{ DB Manager
class dbmanager {
  str dbpath;
  str logpath;
  ihash<str, dbns, &dbns::name, &dbns::hlink> dbs;
  void dbcloser (dbns *db);
public:
  dbmanager (str, str);
  ~dbmanager ();
  const str &getdbpath () { return dbpath; };

  dbns *get (const str &n) { return dbs[n]; };
  dbns *createdb (const str &n, bool aux);
};

void
mkdir_wrapper (str path)
{
  // Make path if necessary
  struct stat sb;
  if (stat (path, &sb) < 0) {
    if (errno == ENOENT) {
      if (mkdir (path, 0755) < 0)
	fatal ("mkdir (%s): %m\n", path.cstr ());
    } else {
      fatal ("stat (%s): %m\n", path.cstr ());
    }
  } else {
    if (!S_ISDIR (sb.st_mode)) 
      fatal ("%s is not a directory\n", path.cstr ());
    if (access (path, W_OK) < 0)
      fatal ("access (%s, W_OK): %m\n", path.cstr ());
  }
}

static str
canonicalize_path (str path)
{
  // Convert path to full path
  char realpath[MAXPATHLEN];
  int fd = -1;
  if (((fd = open (".", O_RDONLY)) >= 0)
     && chdir (path) >= 0)
  {
    if (getcwd (realpath, sizeof (realpath)))
      errno = 0;
    else if (!errno)
      errno = EINVAL;
    if (fchdir (fd))
      warn ("fchdir: %m\n");
  }
  return str (realpath);
}

dbmanager::dbmanager (str p, str lp = NULL) :
  dbpath (p),
  logpath (lp)
{
  mkdir_wrapper (dbpath);
  if (logpath)
    mkdir_wrapper (logpath);

  // Add trailing slash if necessary
  str x = canonicalize_path (dbpath);
  if (x[x.len () - 1] != '/')
    dbpath = strbuf () << x << "/";
  else 
    dbpath = x;
  logpath = canonicalize_path (logpath);
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
    if (logpath)
      mkdir_wrapper (strbuf() << logpath << "/" << n);
    db = New dbns (dbpath, n, aux, logpath);
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

  DBT data;
  bzero (&data, sizeof (data));
  data.data = arg->data.base ();
  data.size = arg->data.size ();

  int r = db->insert (arg->key, data, arg->auxdata, arg->expiration);
  io_finish (t, strbuf("store %s", arg->name.cstr ()));
  adb_status stat = ADB_OK;
  switch (r) {
    case 0:
      break;
    case ENOSPC:
      stat = ADB_DISKFULL;
      break;
    default:
      stat = ADB_ERR;
      break;
  }
  sbp->replyref (stat);

  if (db->quotacheck (quota) > expire_threshold) {
    u_int64_t t = io_start ();
    db->expire (expire_batch_size);
    io_finish (t, strbuf ("expire %s", db->getname ().cstr ()));
  }
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
// {{{ do_expire
void
do_expire (dbmanager *dbm, svccb *sbp)
{
  adb_expirearg *arg = sbp->Xtmpl getarg<adb_expirearg> ();
  adb_status res (ADB_OK);
  dbns *db = dbm->get (arg->name);
  if (!db) {
    res = ADB_ERR;
  } else {
    u_int64_t t = io_start ();
    db->expire (arg->limit, arg->deadline);
    io_finish (t, strbuf ("expire %s", arg->name.cstr ()));
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
  case ADBPROC_EXPIRE:
    do_expire (dbm, sbp);
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
  warnx << "Usage: adbd -d db -S sock [-D] [-q quota]\n";
  exit (0);
}

int 
main (int argc, char **argv)
{

  setprogname (argv[0]);
  mp_set_memory_functions (NULL, simple_realloc, NULL);

  char ch;
  str db_name = "/var/tmp/db";
  str log_path = NULL;
  dbsock = "/tmp/db-sock";

  bool do_daemonize (false);

  while ((ch = getopt (argc, argv, "Dd:l:q:S:"))!=-1)
    switch (ch) {
    case 'D':
      do_daemonize = true;
      break;
    case 'd':
      db_name = optarg;
      break;
    case 'l':
      log_path = optarg;
      break;
    case 'q':
      {
	u_int64_t factor = u_int64_t (1024) * 1024 * 1024;
	char f = optarg[strlen (optarg) - 1];
	if (!isdigit (f))
	  optarg[strlen (optarg) - 1] = 0;
	bool ok = convertint (optarg, &quota);
	if (!ok)
	  usage ();
	switch (f) {
	  case 'b':
	    factor = 1;
	    break;
	  case 'K':
	    factor = 1024;
	    break;
	  case 'M':
	    factor = 1024 * 1024;
	    break;
	  case 'G':
	    factor = u_int64_t (1024) * 1024 * 1024;
	    break;
	  default:
	    if (!isdigit (f))
	      fatal ("Unknown conversion factor '%c'\n", f);
	}
	quota *= factor;
      }
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
  if (log_path)
    warn << "log path: " << log_path << "\n";
  warn << "db socket: " << dbsock << "\n";

  dbm = New dbmanager (db_name, log_path);

  sigcb (SIGHUP, wrap (&halt));
  sigcb (SIGINT, wrap (&halt));
  sigcb (SIGTERM, wrap (&halt));

  sigcb (SIGUSR1, wrap (&toggle_iotime));

  bzero (&master_metadata, sizeof (master_metadata));
  master_metadata.data = strdup ("MASTER_INFO");
  master_metadata.size = strlen ("MASTER_INFO");

  //setup the asrv
  listen_unix (dbsock, dbm);

  amain ();
}

// vim: foldmethod=marker
