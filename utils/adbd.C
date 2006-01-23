#include <async.h>
#include <arpc.h>
#include <wmstr.h>
#include <ihash.h>
#include <sha1.h>

#include <adb_prot.h>
#include <id_utils.h>
#include <dbfe.h>

// {{{ Globals
static bool dbstarted (false);
static str dbsock;
static int clntfd (-1);

static const u_int32_t asrvbufsize (1024*1025);

class dbmanager;
static ptr<dbmanager> dbm;
// }}}
// {{{ DB key conversion
str
id_to_str (const chordID &key)
{
  // pad out all keys to 20 bytes so that they sort correctly
  str keystr = strbuf () << key;
  strbuf padkeystr;
  for (int pad = 2*sha1::hashsize - keystr.len (); pad > 0; pad--)
    padkeystr << "0";
  padkeystr << keystr;
  assert (padkeystr.tosuio ()->resid () == 2*sha1::hashsize);
  return padkeystr;
}

inline DBT
str_to_dbt (const str &s)
{
  DBT d;
  bzero (&d, sizeof (d));
  d.size = s.len ();
  d.data = (void *) s.cstr ();
  return d;
}

inline chordID
dbt_to_id (const DBT &dbt)
{
  str c (static_cast<char *> (dbt.data), dbt.size);
  chordID id;
  if (!str2chordID (c, id))
    fatal << "Invalid chordID as database key: " << c << "\n";
  return id;
}
// }}}

// {{{ DB for Namespace
/* For each namespace (e.g. vnode + ctype, and shared cache),
 * we potentially need to store several things:
 *   1. chordID -> data.  20 bytes -> potentially large bytes
 *   2. chordID -> auxdata (e.g. noauth hash). 20 bytes -> 4 bytes-ish
 *   3. BSM data (chordID -> [vnodeid, ...]. 20 bytes -> 6*16 bytes-ish
 *                vnodeid -> [chordID, ...])
 * with potentially the ability to (3) ordered by len([vnodeid...]).
 * This object encapsulates all that stuff.
 */ 

// {{{ getnumreplicas: Secondary key extractor for bsm data
static int
getnumreplicas (DB *sdb, const DBT *pkey, const DBT *pdata, DBT *skey)
{
  // XXX is returning a pointer to this variable ok???
  //     if we alloc mem here, who would free it???
  static u_int32_t numreps;

  bzero (skey, sizeof (DBT));
  adb_vbsinfo_t vbs;
  if (!buf2xdr (vbs, pdata->data, pdata->size)) {
    hexdump hd (pdata->data, pdata->size);
    warn << "getnumreplicas: unable to unmarshal pdata.\n" 
          << hd << "\n";
    return -1;
  }
  numreps = htonl (vbs.d.size ());
  skey->data = &numreps;
  skey->size = sizeof (numreps);
  return 0;
}
// }}}
// {{{ BerkeleyDB transaction API versioning wrappers
static inline int
dbns_txn_begin (DB_ENV *dbe, DB_TXN **t)
{
  int r;
#if DB_VERSION_MAJOR >= 4
  r = dbe->txn_begin (dbe, NULL, t, 0);
#else
  r = txn_begin (dbe, NULL, t, 0);
#endif
  return r;
}

static inline int
dbns_txn_abort (DB_ENV *dbe, DB_TXN *t)
{
  int r;
#if DB_VERSION_MAJOR >= 4
  r = t->abort (t);
#else
  r = txn_abort (t, 0);
#endif 
  return r;
}

static inline int
dbns_txn_commit (DB_ENV *dbe, DB_TXN *t)
{
  int r;
#if DB_VERSION_MAJOR >= 4
  r = t->commit (t, 0);
#else
  r = txn_commit (t, 0);
#endif 
  return r;
}
// }}}

class dbns {
  friend class dbmanager;

  str name;
  ihash_entry<dbns> hlink;

  DB_ENV *dbe;

  DB *datadb;
  DB *auxdatadb;
  DB *bsdb;
  DB *bsdx;

  // XXX should have a instance-level lock that governs datadb/auxdatadb.

  void warner (const char *method, const char *desc, int r);

public:
  dbns (const str &name, bool aux);
  ~dbns ();

  void sync ();

  bool hasaux () { return auxdatadb != NULL; };

  // Primary data management
  int insert (const chordID &key, const str data, u_int32_t auxdata);
  int insert (const chordID &key, DBT &data, DBT &auxdata);
  int lookup (const chordID &key, str &data);
  int del (const chordID &key);
  int getkeys (const chordID &start, size_t count, bool getaux,
      rpc_vec<adb_keyaux_t, RPC_INFINITY> &out); 

  // Block status information
  int getblockrange_all (const chordID &start, const chordID &stop,
    int count, rpc_vec<adb_bsloc_t, RPC_INFINITY> &out);
  int getblockrange_extant (const chordID &start, const chordID &stop,
    int extant, int count, rpc_vec<adb_bsloc_t, RPC_INFINITY> &out);
  int getkeyson (const adb_vnodeid &n, const chordID &start,
      const chordID &stop, rpc_vec<adb_keyaux_t, RPC_INFINITY> &out);
  int update (const chordID &block, const adb_bsinfo_t &bsinfo, bool present);
  int getinfo (const chordID &block, rpc_vec<adb_vnodeid, RPC_INFINITY> &out);
};

// {{{ dbns::dbns
dbns::dbns (const str &name, bool aux) :
  name (name),
  dbe (NULL),
  datadb (NULL),
  auxdatadb (NULL),
  bsdb (NULL),
  bsdx (NULL)
{
#define DBNS_ERRCHECK(desc) \
  if (r) {		  \
    fatal << desc << " returned " << r << ": " << db_strerror (r) << "\n"; \
    return;		  \
  }

  int r = -1;
  r = dbfe_initialize_dbenv (&dbe, name, false, 1024);
  DBNS_ERRCHECK ("dbe->open");

  r = dbfe_opendb (dbe, &datadb, "db", DB_CREATE, 0);
  DBNS_ERRCHECK ("datadb->open");
  if (aux) {
    r = dbfe_opendb (dbe, &auxdatadb, "auxdb", DB_CREATE, 0);
    DBNS_ERRCHECK ("auxdatadb->open");
  }

  r = dbfe_opendb (dbe, &bsdb, "bsdb", DB_CREATE, 0);
  DBNS_ERRCHECK ("bsdb->open");
  r = dbfe_opendb (dbe, &bsdx, "bsdx", DB_CREATE, 0, /* dups = */ true);
  DBNS_ERRCHECK ("bsdx->open");
  r = bsdb->associate (bsdb, NULL, bsdx, getnumreplicas, DB_AUTO_COMMIT);
  DBNS_ERRCHECK ("bsdb->associate (bsdx)");
  warn << "dbns::dbns (" << name << ", " << aux << ")\n";
}
// }}}
// {{{ dbns::~dbns
dbns::~dbns ()
{
#define DBNS_DBCLOSE(x)			\
  if (x) {				\
    (void) x->close (x, 0); x = NULL;	\
  }
  // Start with main databases
  DBNS_DBCLOSE(datadb);
  DBNS_DBCLOSE(auxdatadb);
  // Close secondary before the primary
  DBNS_DBCLOSE(bsdx);
  DBNS_DBCLOSE(bsdb);
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
  warn << method << ": " << desc << ": " << db_strerror (r) << "\n";
}
// }}}
// {{{ dbns::sync
void
dbns::sync ()
{
#if (DB_VERSION_MAJOR < 4)
   txn_checkpoint (dbe, 0, 0, 0);
#else
   dbe->txn_checkpoint (dbe, 0, 0, 0);
#endif
}
// }}}
// {{{ dbns::insert (chordID, DBT, DBT)
int
dbns::insert (const chordID &key, DBT &data, DBT &auxdata)
{
  int r = 0;
  str key_str = id_to_str (key);
  DBT skey = str_to_dbt (key_str);

  if (auxdatadb) {
    // To keep auxdata in sync, use an explicit transaction
    DB_TXN *t = NULL;
    r = dbns_txn_begin (dbe, &t);
    r = datadb->put (datadb, t, &skey, &data, 0);
    if (r) {
      warner ("dbns::insert", "data put error", r);
      r = dbns_txn_abort (dbe, t);
    } 
    r = auxdatadb->put (auxdatadb, t, &skey, &auxdata, 0);
    if (r) {
      warner ("dbns::insert", "auxdata put error", r);
      r = dbns_txn_abort (dbe, t);
    } else {
      r = dbns_txn_commit (dbe, t);
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
// {{{ dbns::insert (chordID, str, u_int32_t)
int
dbns::insert (const chordID &key, const str data, u_int32_t auxdata)
{
  DBT datadbt = str_to_dbt (data);
  DBT auxdatadbt;
  bzero (&auxdatadbt, sizeof (auxdatadbt));
  auxdatadbt.size = sizeof (u_int32_t);
  u_int32_t nauxdata = htonl (auxdata);
  auxdatadbt.data = &nauxdata; 
  return insert (key, data, auxdata);
}
// }}}
// {{{ dbns::lookup
int
dbns::lookup (const chordID &key, str &data)
{
  int r = 0;

  str key_str = id_to_str (key);
  DBT skey = str_to_dbt (key_str);

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
// {{{ dbns::del
int
dbns::del (const chordID &key)
{
  int r = 0;
  str key_str = id_to_str (key);
  DBT skey = str_to_dbt (key_str);
  // Implicit transaction
  r = datadb->del (datadb, NULL, &skey, DB_AUTO_COMMIT);

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
  // Fully serialized reads of auxdatadb
  if (auxdatadb) 
    r = auxdatadb->cursor (auxdatadb, NULL, &cursor, 0);
  else
    r = datadb->cursor (datadb, NULL, &cursor, 0);

  if (r) {
    warner ("dbns::getkeys", "cursor open", r);
    (void) cursor->c_close (cursor);
    return r;
  }

  adb_keyaux_t keyaux;

  // Could possibly improve efficiency here by using SleepyCat's bulk reads
  str key_str = id_to_str (start);
  DBT key = str_to_dbt (key_str);
  DBT data_template;
  bzero (&data_template, sizeof (data_template));
  // If DB_DBT_PARTIAL and data.dlen == 0, no data is read.
  if (!auxdatadb || !getaux)
    data_template.flags = DB_DBT_PARTIAL;
  DBT data = data_template;

  r = cursor->c_get (cursor, &key, &data, DB_SET_RANGE);
  while (out.size () < count && !r) {
    keyaux.key = dbt_to_id (key);
    if (getaux)
      keyaux.auxdata = ntohl (*(u_int32_t *)data.data);
    out.push_back (keyaux);

    bzero (&key, sizeof (key));
    data = data_template;
    r = cursor->c_get (cursor, &key, &data, DB_NEXT);
  }
  if (r && r != DB_NOTFOUND)
    warner ("dbns::getkeys", "cursor get", r);
  (void) cursor->c_close (cursor);

  return r;
}
// }}}
// {{{ dbns::getblockrange
int
dbns::getblockrange_all (const chordID &start, const chordID &stop,
    int count, rpc_vec<adb_bsloc_t, RPC_INFINITY> &out)
{
  int r = 0;
  DBC *cursor;
  r = bsdb->cursor (bsdb, NULL, &cursor,0);
  if (r) {
    warner ("dbns::getblockrange_all", "cursor open", r);
    (void) cursor->c_close (cursor);
    return r;
  }

  chordID cur = start;

  str key_str = id_to_str (start);
  DBT key = str_to_dbt (key_str);
  DBT data;
  bzero (&data, sizeof (data));

  u_int32_t limit = count;
  if (count < 0)
    limit = (asrvbufsize/(sizeof (adb_bsloc_t)/2));

  r = cursor->c_get (cursor, &key, &data, DB_SET_RANGE);
  // Loop around the ring if at end.
  if (r == DB_NOTFOUND) {
    bzero (&key, sizeof (key));
    r = cursor->c_get (cursor, &key, &data, DB_FIRST);
  }
  while (!r && out.size () < limit)
  {
    adb_vbsinfo_t vbs;
    chordID k = dbt_to_id (key);
    if (!betweenrightincl (cur, stop, k)) {
      r = DB_NOTFOUND;
      break;
    }
    cur = k;
    if (buf2xdr (vbs, data.data, data.size)) {
      adb_bsloc_t bsloc;
      bsloc.block = k;
      bsloc.hosts = vbs.d;
      out.push_back (bsloc);
    } else {
      warner ("dbns::getblockrange_all", "XDR unmarshalling failed", 0);
    } 
    bzero (&key, sizeof (key));
    bzero (&data, sizeof (data));
    r = cursor->c_get (cursor, &key, &data, DB_NEXT);
    if (r == DB_NOTFOUND)
      r = cursor->c_get (cursor, &key, &data, DB_FIRST);
  }
  if (r && r != DB_NOTFOUND)
    warner ("dbns::getblockrange_all", "cursor get", r);

  (void) cursor->c_close (cursor);
  return r;
}

int
dbns::getblockrange_extant (const chordID &start, const chordID &stop,
    int extant, int count, rpc_vec<adb_bsloc_t, RPC_INFINITY> &out)
{
  // XXX maybe think of a way to improve the degree of abstraction.
  //     for cursors.
  int r = 0;
  DBC *cursor;
  r = bsdx->cursor (bsdx, NULL, &cursor,0);
  if (r) {
    warner ("dbns::getblockrange_extant", "cursor open", r);
    (void) cursor->c_close (cursor);
    return r;
  }

  extant = htonl (extant);
  DBT ekey;
  bzero (&ekey, sizeof (ekey));
  ekey.data = &extant;
  ekey.size = sizeof (extant);

  DBT key;
  bzero (&key, sizeof (key));
  DBT data;
  bzero (&data, sizeof (data));

  u_int32_t limit = count;
  if (count < 0)
    limit = (asrvbufsize/(sizeof (adb_bsloc_t)/2));

  // Find only those who have extant as requested 
  r = cursor->c_pget (cursor, &ekey, &key, &data, DB_SET);
  while (!r && out.size () < limit)
  {
    adb_vbsinfo_t vbs;
    chordID k = dbt_to_id (key);
    // grab only those keys that are interesting to caller.
    if (!betweenbothincl (start, stop, k))
      goto next;
    if (buf2xdr (vbs, data.data, data.size)) {
      adb_bsloc_t bsloc;
      bsloc.block = k;
      bsloc.hosts = vbs.d;
      out.push_back (bsloc);
    } else {
      warner ("dbns::getblockrange_extant", "XDR unmarshalling failed", 0);
    } 
  next:
    bzero (&key, sizeof (key));
    bzero (&data, sizeof (data));
    r = cursor->c_pget (cursor, &ekey, &key, &data, DB_NEXT_DUP);
  }
  if (r && r != DB_NOTFOUND)
    warner ("dbns::getblockrange_extant", "cursor get", r);

  (void) cursor->c_close (cursor);
  return r;
}
// }}}
// {{{ dbns::getkeyson
int
dbns::getkeyson (const adb_vnodeid &n, const chordID &start,
    const chordID &stop, rpc_vec<adb_keyaux_t, RPC_INFINITY> &out)
{
  int r = 0;
  DBC *cursor;

  // Fully serialized reads of bsdb
  r = bsdb->cursor (bsdb, NULL, &cursor, 0);
  if (r) {
    warner ("dbns::getkeyson", "cursor open", r);
    (void) cursor->c_close (cursor);
    return r;
  }

  adb_keyaux_t keyaux;
  chordID cur = start;

  // Could possibly improve efficiency here by using SleepyCat's bulk reads
  str key_str = id_to_str (start);
  DBT key = str_to_dbt (key_str);
  DBT data;
  bzero (&data, sizeof (data));

  // Position the cursor if possible
  r = cursor->c_get (cursor, &key, &data, DB_SET_RANGE);
  // Also must remember to start to loop around the ring
  if (r == DB_NOTFOUND) {
    bzero (&key, sizeof (key));
    r = cursor->c_get (cursor, &key, &data, DB_FIRST);
  }
  // Each adb_keyaux_t is 24ish bytes; leave some slack
  while (!r &&
         out.size () < (asrvbufsize/(1.5*sizeof (adb_keyaux_t))))
  {
    adb_vbsinfo_t vbs;
    keyaux.key = dbt_to_id (key);
    if (!betweenrightincl (cur, stop, keyaux.key)) {
      r = DB_NOTFOUND;
      break;
    }
    cur = keyaux.key;
    if (buf2xdr (vbs, data.data, data.size)) {
      size_t dx;
      for (dx = 0; dx < vbs.d.size (); dx++) {
	if (memcmp (&vbs.d[dx].n, &n, sizeof (n)) == 0) break;
      }
      if (dx < vbs.d.size ()) {
	keyaux.auxdata = vbs.d[dx].auxdata;
	out.push_back (keyaux);
      }
    } else {
      warner ("dbns::getkeyson", "XDR unmarshalling failed", 0);
    } 

    bzero (&key, sizeof (key));
    bzero (&data, sizeof (data));
    r = cursor->c_get (cursor, &key, &data, DB_NEXT);
    if (r == DB_NOTFOUND)
      r = cursor->c_get (cursor, &key, &data, DB_FIRST);
  }
  if (r && r != DB_NOTFOUND)
    warner ("dbns::getkeyson", "cursor get", r);

  (void) cursor->c_close (cursor);
  return r;
}
// }}}
// {{{ dbns::update
int
dbns::update (const chordID &key, const adb_bsinfo_t &bsinfo, bool present)
{
  int r;
  str key_str = id_to_str (key);
  DBT skey = str_to_dbt (key_str);
  DBT data; bzero (&data, sizeof (data));
  adb_vbsinfo_t vbs;

  DB_TXN *t = NULL;
  r = dbns_txn_begin (dbe, &t);
  // get, with a write lock.
  r = bsdb->get (bsdb, t, &skey, &data, DB_RMW);
  if (r && r != DB_NOTFOUND) {
    warner ("dbns::update", "get error", r);
    dbns_txn_abort (dbe, t);
    return r;
  }
  // append/change
  if (r == DB_NOTFOUND) {
    if (present) {
      vbs.d.push_back (bsinfo);
    } else {
      // Nothing to do, go home.
      dbns_txn_commit (dbe, t);
      return 0;
    }
  } else {
    if (!buf2xdr (vbs, data.data, data.size)) {
      warner ("dbns::update", "XDR unmarshalling failed", 0);
      // Nuke the old corrupt, soft state data.
      vbs.d.clear ();
    }
    // Find index of bsinfo in vbs.
    size_t dx; 
    for (dx = 0; dx < vbs.d.size (); dx++) 
      if (memcmp (&vbs.d[dx].n, &bsinfo.n, sizeof (bsinfo.n)) == 0) break;
    if (present) {
      if (dx < vbs.d.size ())
	vbs.d[dx].auxdata = bsinfo.auxdata;
      else
	vbs.d.push_back (bsinfo);
    } else {
      if (dx < vbs.d.size ()) {
	vbs.d[dx] = vbs.d.back ();
	vbs.d.pop_back ();
      } else {
	// No need to re-write to db; nothing changed.
	dbns_txn_commit (dbe, t);
	return r;
      }
    }
  }
  // put
  str vbsout = xdr2str (vbs);
  data = str_to_dbt (vbsout);
  r = bsdb->put (bsdb, t, &skey, &data, 0);
  if (r) {
    warner ("dbns::update", "put error", r);
    dbns_txn_abort (dbe, t);
    return r;
  }
  // commit
  r = dbns_txn_commit (dbe, t);
  return r;
}
// }}}
// {{{ dbns::getinfo
int
dbns::getinfo (const chordID &key, rpc_vec<adb_vnodeid, RPC_INFINITY> &out)
{
  int r;
  str key_str = id_to_str (key);
  DBT skey = str_to_dbt (key_str);
  DBT data; bzero (&data, sizeof (data));
  adb_vbsinfo_t vbs;

  // get
  r = bsdb->get (bsdb, NULL, &skey, &data, 0);
  if (r && r != DB_NOTFOUND) {
    warner ("dbns::update", "get error", r);
    return r;
  }
  if (r == DB_NOTFOUND) {
    return 0;
  } else {
    if (!buf2xdr (vbs, data.data, data.size)) {
      warner ("dbns::update", "XDR unmarshalling failed", 0);
      // Nuke the old corrupt, soft state data.
      vbs.d.clear ();
      return 0;
    }
    for (size_t i = 0; i < vbs.d.size (); i++)
      out.push_back (vbs.d[i].n);
  }
  return 0;
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
    db->sync ();
  }
  delete db;
  db = NULL;
}

dbns *
dbmanager::createdb (const str &n, bool aux)
{
  dbns *db = dbs[n];
  if (!db) {
    db = New dbns (n, aux);
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
  dbm = NULL;
  exit (0);
}
// }}}

// {{{ RPC execution
// {{{ do_initspace
void
do_initspace (ptr<dbmanager> dbm, svccb *sbp)
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
do_store (ptr<dbmanager> dbm, svccb *sbp)
{
  adb_storearg *arg = sbp->Xtmpl getarg<adb_storearg> ();
  dbns *db = dbm->get (arg->name);
  if (!db) {
    sbp->replyref (ADB_ERR);
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
  sbp->replyref ((r == 0) ? ADB_OK : ADB_ERR);
}
// }}}
// {{{ do_fetch
void
do_fetch (ptr<dbmanager> dbm, svccb *sbp)
{
  adb_fetcharg *arg = sbp->Xtmpl getarg<adb_fetcharg> ();
  adb_fetchres res (ADB_OK); 

  dbns *db = dbm->get (arg->name);
  if (!db) {
    res.set_status (ADB_ERR);
    sbp->replyref (res);
    return;
  }
 
  str data; 
  int r = db->lookup (arg->key, data);
  if (r) {
    res.set_status ((r == DB_NOTFOUND ? ADB_NOTFOUND : ADB_ERR));
  } else {
    res.resok->key = arg->key;
    res.resok->data = data;
  }

  sbp->replyref (res);
}
// }}}
// {{{ do_getkeys
void
do_getkeys (ptr<dbmanager> dbm, svccb *sbp)
{
  adb_getkeysarg *arg = sbp->Xtmpl getarg<adb_getkeysarg> ();
  adb_getkeysres res (ADB_OK);

  dbns *db = dbm->get (arg->name);
  if (!db) {
    res.set_status (ADB_ERR);
    sbp->replyref (res);
    return;
  }
  // Gets up to 128 keys at a time.
  res.resok->hasaux = arg->getaux;
  int r = db->getkeys (arg->start, 128, arg->getaux, res.resok->keyaux);
  res.resok->complete = (r == DB_NOTFOUND);
  if (r && r != DB_NOTFOUND) 
    res.set_status (ADB_ERR);
  sbp->replyref (res);
}
// }}}
// {{{ do_delete
void
do_delete (ptr<dbmanager> dbm, svccb *sbp)
{
  adb_deletearg *arg = sbp->Xtmpl getarg<adb_deletearg> ();
  dbns *db = dbm->get (arg->name);
  if (!db) {
    sbp->replyref (ADB_ERR);
    return;
  }
  int r = db->del (arg->key);
  sbp->replyref ((r == 0) ? ADB_OK : ADB_NOTFOUND);
}
// }}}
// {{{ do_getblockrange
void
do_getblockrange (ptr<dbmanager> dbm, svccb *sbp)
{
  adb_getblockrangearg *arg = sbp->Xtmpl getarg<adb_getblockrangearg> ();
  adb_getblockrangeres res;
  dbns *db = dbm->get (arg->name);
  if (!db) {
    res.status = ADB_ERR;
    sbp->replyref (res);
    return;
  }
  int r (0);
  res.status = ADB_OK;
  if (arg->extant >= 0) {
    r = db->getblockrange_extant (arg->start, arg->stop,
	  arg->extant, arg->count, res.blocks);
  } else {
    r = db->getblockrange_all (arg->start, arg->stop,
	  arg->count, res.blocks);
  }
  if (r) {
    if (r == DB_NOTFOUND) {
      res.status = ADB_COMPLETE;
    } else {
      res.status = ADB_ERR;
      res.blocks.clear ();
    }
  }
  sbp->replyref (res);
}
// }}}
// {{{ do_getkeyson
void
do_getkeyson (ptr<dbmanager> dbm, svccb *sbp)
{
  adb_getkeysonarg *arg = sbp->Xtmpl getarg<adb_getkeysonarg> ();
  adb_getkeysres res (ADB_OK);
  dbns *db = dbm->get (arg->name);
  if (!db) {
    res.set_status (ADB_ERR);
    sbp->replyref (res);
    return;
  }
  res.resok->hasaux = db->hasaux ();

  int r = db->getkeyson (arg->who, arg->start, arg->stop, res.resok->keyaux);
  res.resok->complete = (r == DB_NOTFOUND);
  if (r && r != DB_NOTFOUND) 
    res.set_status (ADB_ERR);
  sbp->replyref (res);
}
// }}}
// {{{ do_update
void
do_update (ptr<dbmanager> dbm, svccb *sbp)
{
  adb_updatearg *arg = sbp->Xtmpl getarg<adb_updatearg> ();
  adb_status res (ADB_OK);
  dbns *db = dbm->get (arg->name);
  if (!db) {
    sbp->replyref (ADB_ERR);
    return;
  }
  int r = db->update (arg->key, arg->bsinfo, arg->present);
  if (r)
    res = ADB_ERR;
  sbp->replyref (res);
}
// }}}
// {{{ do_getinfo
void
do_getinfo (ptr<dbmanager> dbm, svccb *sbp)
{
  adb_getinfoarg *arg = sbp->Xtmpl getarg<adb_getinfoarg> ();
  adb_getinfores res;
  res.status = ADB_OK;
  dbns *db = dbm->get (arg->name);
  if (!db) {
    sbp->replyref (ADB_ERR);
    return;
  }
  int r = db->getinfo (arg->key, res.nlist);
  if (r) {
    res.status = ADB_ERR;
    res.nlist.clear ();
  }
  sbp->replyref (res);
}
// }}}
// }}}

// {{{ RPC accept and dispatch
void
dispatch (ref<axprt_stream> s, ptr<asrv> a, ptr<dbmanager> dbm, svccb *sbp)
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
  case ADBPROC_GETBLOCKRANGE:
    do_getblockrange (dbm, sbp);
    break;
  case ADBPROC_GETKEYSON:
    do_getkeyson (dbm, sbp);
    break;
  case ADBPROC_UPDATE:
    do_update (dbm, sbp);
    break;
  case ADBPROC_GETINFO:
    do_getinfo (dbm, sbp);
    break;
  default:
    fatal << "unknown procedure: " << sbp->proc () << "\n";
  }
  
  return;
}

static void accept_cb (int lfd, ptr<dbmanager> dbm);
static void
listen_unix (str sock_name, ptr<dbmanager> dbm)
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
accept_cb (int lfd, ptr<dbmanager> dbm)
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

  ptr<dbmanager> dbm = New refcounted<dbmanager> (db_name);

  sigcb (SIGHUP, wrap (&halt));
  sigcb (SIGINT, wrap (&halt));
  sigcb (SIGTERM, wrap (&halt));

  //setup the asrv
  listen_unix (dbsock, dbm);

  amain ();
}

// vim: foldmethod=marker
