#include <async.h>
#include <arpc.h>
#include <wmstr.h>
#include <ihash.h>
#include <sha1.h>

#include <adb_prot.h>
#include <keyauxdb.h>
#include <id_utils.h>
#include <dbfe.h>
#include <merkle_tree_disk.h>

// {{{ Globals
static bool dbstarted (false);
static str dbsock;
static int clntfd (-1);

static const u_int32_t asrvbufsize (1024*1025);

class dbmanager;
static dbmanager *dbm;
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
 *
 * The goal is to support the following operations:
 *   A. lsd merkle tree: fast read, any order key+auxdata, all keys from 1, 2
 *   B. syncd merkle tree: fast read, any order key+auxdata, one vnode, from 3
 *   C. pmaint: slow read ok, find next key in order
 *   D. repair: find $n$ least replicated blocks between (a, b], from 3
 * cursor reads of 1 and 2 will be sorted.
 *
 */ 

// {{{ getnumreplicas: Secondary key extractor for bsm data
static int
getnumreplicas (DB *sdb, const DBT *pkey, const DBT *pdata, DBT *skey)
{
  u_int32_t *numreps = (u_int32_t *) malloc(sizeof(u_int32_t));

  adb_vbsinfo_t vbs;
  if (!buf2xdr (vbs, pdata->data, pdata->size)) {
    hexdump hd (pdata->data, pdata->size);
    warn << "getnumreplicas: unable to unmarshal pdata.\n" 
          << hd << "\n";
    return -1;
  }
  *numreps = htonl (vbs.d.size ());
  // this flag should mean that berkeley db will free this memory when it's
  // done with it.
  skey->flags = DB_DBT_APPMALLOC;
  skey->data = numreps;
  skey->size = sizeof (*numreps);
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
// {{{ dbns declarations
class dbns {
  friend class dbmanager;

  str name;
  ihash_entry<dbns> hlink;

  DB_ENV *dbe;

  DB *datadb;
  DB *auxdatadb;
  DB *bsdb;
  DB *bsdx;

  keyauxdb *kdb;
  merkle_tree *mtree;

  // XXX should have a instance-level lock that governs datadb/auxdatadb.

  // Private updater, for different txn handling
  int updateone (const chordID &key, const adb_bsinfo_t &bsinfo, bool present,
	         DB_TXN *t);

public:
  dbns (const str &dbpath, const str &name, bool aux);
  ~dbns ();

  void warner (const char *method, const char *desc, int r);

  void sync ();

  bool hasaux () { return auxdatadb != NULL; };

  // Primary data management
  bool kinsert (const chordID &key, u_int32_t auxdata);
  int insert (const chordID &key, DBT &data, DBT &auxdata);
  int lookup (const chordID &key, str &data);
  int lookup_nextkey (const chordID &key, chordID &nextkey);
  int del (const chordID &key, u_int32_t auxdata);
  int kgetkeys (u_int32_t start, size_t count, bool getaux,
      rpc_vec<adb_keyaux_t, RPC_INFINITY> &out); 
  int getkeys (const chordID &start, size_t count, bool getaux,
      rpc_vec<adb_keyaux_t, RPC_INFINITY> &out); 
  int migrate_getkeys (void);

  // Block status information
  int getblockrange_all (const chordID &start, const chordID &stop,
    int count, rpc_vec<adb_bsloc_t, RPC_INFINITY> &out);
  int getblockrange_extant (const chordID &start, const chordID &stop,
    int extant, int count, rpc_vec<adb_bsloc_t, RPC_INFINITY> &out);
  int getkeyson (const adb_vnodeid &n, const chordID &start,
		 const chordID &stop, int count, 
		 rpc_vec<adb_keyaux_t, RPC_INFINITY> &out);
  int update (const chordID &block, const adb_bsinfo_t &bsinfo, bool present);
  int updatebatch (rpc_vec<adb_updatearg, RPC_INFINITY> &uargs);
  int getinfo (const chordID &block, rpc_vec<adb_vnodeid, RPC_INFINITY> &out);
};
// }}}
// {{{ dbns::dbns
dbns::dbns (const str &dbpath, const str &name, bool aux) :
  name (name),
  dbe (NULL),
  datadb (NULL),
  auxdatadb (NULL),
  bsdb (NULL),
  bsdx (NULL),
  kdb (NULL)
{
#define DBNS_ERRCHECK(desc) \
  if (r) {		  \
    fatal << desc << " returned " << r << ": " << db_strerror (r) << "\n"; \
    return;		  \
  }
  assert (dbpath[dbpath.len () - 1] == '/');
  strbuf fullpath ("%s%s", dbpath.cstr (), name.cstr ());

  int r = -1;
  r = dbfe_initialize_dbenv (&dbe, fullpath, false, 1024);
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
  warn << "dbns::dbns (" << dbpath << ", " << name << ", " << aux << ")\n";

  str fullpath_kdb = strbuf() << fullpath << "/kdb.db";
  kdb = New keyauxdb (fullpath_kdb);
  u_int32_t dummy;
  if (NULL == kdb->getkeys (0, 1, &dummy) ) {
    // This could take a very long time if there's a lot of data.
    migrate_getkeys ();
  }

  // now make the on disk merkle tree, migrating keys as necessary
  str mtree_index = strbuf() << fullpath << "/index.mrk";
  FILE *f = fopen (mtree_index, "r");
  bool mtree_exists = true;
  if (f == NULL) {
    mtree_exists = false;
  } else {
    fclose (f);
  }

  mtree = New merkle_tree_disk
     (mtree_index,
      strbuf() << fullpath << "/internal.mrk",
      strbuf() << fullpath << "/leaf.mrk", true /*read-write*/);
  
  if (!mtree_exists) {
    uint avail;
    uint at_a_time = 100000;
    const keyaux_t *keys;
    uint recno = 0;
    while ((keys = kdb->getkeys (recno, at_a_time, &avail)) && avail > 0) {
      for (uint j = 0; j < avail; j++) {
	chordID k;
	uint aux;
	keyaux_unmarshall (&(keys[j]), &k, &aux);
	if (hasaux ()) {
	  mtree->insert (k, aux);
	} else {
	  mtree->insert (k);
	}
      }
      recno += avail;
    }
    warn << "Migrated " << recno << " records to the mtree\n";
  }

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
  delete kdb;
  kdb = NULL;
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
dbns::sync ()
{
#if (DB_VERSION_MAJOR < 4)
   txn_checkpoint (dbe, 0, 0, 0);
#else
   dbe->txn_checkpoint (dbe, 0, 0, 0);
#endif
   kdb->sync ();
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
  kdb->addkey (key, auxdata);
  return true;
}
// }}}
// {{{ dbns::insert (chordID, DBT, DBT)
int
dbns::insert (const chordID &key, DBT &data, DBT &auxdata)
{
  int r = 0;
  str key_str = id_to_str (key);
  DBT skey;
  str_to_dbt (key_str, &skey);

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
// {{{ dbns::lookup
int
dbns::lookup (const chordID &key, str &data)
{
  int r = 0;

  str key_str = id_to_str (key);
  DBT skey;
  str_to_dbt (key_str, &skey);

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

  str key_str = id_to_str (key);
  DBT skey;
  str_to_dbt (key_str, &skey);

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
  str key_str = id_to_str (key);
  DBT skey;
  str_to_dbt (key_str, &skey);
  // Implicit transaction
  r = datadb->del (datadb, NULL, &skey, DB_AUTO_COMMIT);
  // XXX Should delkey from kdb.

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
  str key_str = id_to_str (start);
  DBT key;
  str_to_dbt (key_str, &key);
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
// {{{ dbns::kgetkeys
int
dbns::kgetkeys (u_int32_t start, size_t count, bool getaux, rpc_vec<adb_keyaux_t, RPC_INFINITY> &out)
{
  u_int32_t n (0);
  const keyaux_t *v = kdb->getkeys (start, count, &n);
  if (!v)
    return DB_NOTFOUND;
  out.setsize (n);
  for (u_int32_t i = 0; i < n; i++) {
    keyaux_unmarshall (&v[i], &out[i].key, &out[i].auxdata);
  }
  return 0;
}
// }}}
// {{{ dbns::migrate_getkeys
int
dbns::migrate_getkeys (void)
{
  warn << "migrate_getkeys (" << name << ")\n";
  int r = 0;
  DBC *cursor;
  // Fully serialized reads of auxdatadb
  if (auxdatadb) 
    r = auxdatadb->cursor (auxdatadb, NULL, &cursor, 0);
  else
    r = datadb->cursor (datadb, NULL, &cursor, 0);

  if (r) {
    warner ("dbns::migrate_getkeys", "cursor open", r);
    (void) cursor->c_close (cursor);
    return r;
  }

  // Could possibly improve efficiency here by using SleepyCat's bulk reads
  str key_str = id_to_str (chordID (0));
  DBT key;
  str_to_dbt (key_str, &key);
  DBT data_template;
  bzero (&data_template, sizeof (data_template));
  // If DB_DBT_PARTIAL and data.dlen == 0, no data is returned.
  // Of course, since BerkeleyDB BTree's puts data and keys
  // on the leaf pages, we are left reading lots of data anyway.
  if (!auxdatadb)
    data_template.flags = DB_DBT_PARTIAL;
  DBT data = data_template;

  r = cursor->c_get (cursor, &key, &data, DB_FIRST);
  while (!r) {
    chordID k = dbt_to_id (key);
    u_int32_t auxdata (0);
    if (auxdatadb)
      auxdata = ntohl (*(u_int32_t *)data.data);

    kdb->addkey (k, auxdata);

    bzero (&key, sizeof (key));
    data = data_template;
    r = cursor->c_get (cursor, &key, &data, DB_NEXT);
  }

  if (r && r != DB_NOTFOUND)
    warner ("dbns::migrate_getkeys", "cursor get", r);
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
  DBT key;
  str_to_dbt (key_str, &key);
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

  // since we set a limit, we know the maximum amount we have to allocate
  out.setsize (limit);
  u_int32_t elements = 0;

  while (!r && elements < limit)
  {
    adb_vbsinfo_t vbs;
    chordID k = dbt_to_id (key);
    if (!betweenrightincl (cur, stop, k)) {
      r = DB_NOTFOUND;
      break;
    }
    cur = k;
    if (buf2xdr (vbs, data.data, data.size)) {
      out[elements].block = k;
      out[elements].hosts.setsize (vbs.d.size());
      // explicit deep copy
      for (u_int32_t i = 0; i < vbs.d.size(); i++) {
	out[elements].hosts[i].n = vbs.d[i].n;
	out[elements].hosts[i].auxdata = vbs.d[i].auxdata;
      }
      elements++;
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

  if (elements < limit) {
    out.setsize (elements);
  }

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

  str key_str = id_to_str (start);
  DBT key;
  str_to_dbt (key_str, &key);
  chordID cur = start;
  DBT data;
  bzero (&data, sizeof (data));

  u_int32_t limit = count;
  if (count < 0)
    limit = (asrvbufsize/(sizeof (adb_bsloc_t)/2));

  // Find only those who have extant as requested
  // find the one with the primary key greater or equal to 
  // the start key
  r = cursor->c_pget (cursor, &ekey, &key, &data, DB_GET_BOTH_RANGE);
  // Also must remember to start to loop around the ring
  if (r == DB_NOTFOUND) {
    bzero (&key, sizeof (key));
    // I guess it didn't work.  Just try any old key with this extant
    r = cursor->c_pget (cursor, &ekey, &key, &data, DB_SET);
  }

  // since we set a limit, we know the maximum amount we have to allocate
  out.setsize (limit);
  u_int32_t elements = 0;

  while (!r && elements < limit)
  {
    adb_vbsinfo_t vbs;
    chordID k = dbt_to_id (key);
    // NOTE: this assumes that the keys are stored in order of the
    // primary keys in the secondary db.  Also assumes the DB_GET_BOTH_RANGE
    // call above works as expected.
    if (!betweenrightincl (cur, stop, k)) {
      r = DB_NOTFOUND;
      break;
    }
    cur = k;
    if (buf2xdr (vbs, data.data, data.size)) {
      out[elements].block = k;
      out[elements].hosts.setsize (vbs.d.size ());
      // explicit deep copy
      for (u_int32_t i = 0; i < vbs.d.size(); i++) {
	out[elements].hosts[i].n = vbs.d[i].n;
	out[elements].hosts[i].auxdata = vbs.d[i].auxdata;
      }
      elements++;
    } else {
      warner ("dbns::getblockrange_extant", "XDR unmarshalling failed", 0);
    } 
    bzero (&key, sizeof (key));
    bzero (&data, sizeof (data));
    r = cursor->c_pget (cursor, &ekey, &key, &data, DB_NEXT_DUP);
    if (r == DB_NOTFOUND)
      r = cursor->c_pget (cursor, &ekey, &key, &data, DB_SET);
  }
  if (r && r != DB_NOTFOUND)
    warner ("dbns::getblockrange_extant", "cursor get", r);

  if (elements < limit) {
    out.setsize (elements);
  }

  (void) cursor->c_close (cursor);
  return r;
}
// }}}
// {{{ dbns::getkeyson
int
dbns::getkeyson (const adb_vnodeid &n, const chordID &start,
    const chordID &stop, int count, rpc_vec<adb_keyaux_t, RPC_INFINITY> &out)
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

  chordID cur = start;

  // Could possibly improve efficiency here by using SleepyCat's bulk reads
  str key_str = id_to_str (start);
  DBT key;
  str_to_dbt (key_str, &key);
  DBT data;
  bzero (&data, sizeof (data));

  u_int32_t limit = count;
  if (count < 0)
    limit = (u_int32_t) (asrvbufsize/(1.5*sizeof (adb_keyaux_t)));

  // Position the cursor if possible
  r = cursor->c_get (cursor, &key, &data, DB_SET_RANGE);
  // Also must remember to start to loop around the ring
  if (r == DB_NOTFOUND) {
    bzero (&key, sizeof (key));
    r = cursor->c_get (cursor, &key, &data, DB_FIRST);
  }

  // since we set a limit, we know the maximum amount we have to allocate
  out.setsize (limit);
  u_int32_t elements = 0;

  // Each adb_keyaux_t is 24ish bytes; leave some slack
  while (!r && elements < limit)
  {
    adb_vbsinfo_t vbs;
    chordID curkey = dbt_to_id (key);
    if (!betweenrightincl (cur, stop, curkey)) {
      r = DB_NOTFOUND;
      break;
    }
    cur = curkey;
    if (buf2xdr (vbs, data.data, data.size)) {
      size_t dx;
      for (dx = 0; dx < vbs.d.size (); dx++) {
	if (memcmp (&vbs.d[dx].n, &n, sizeof (n)) == 0) break;
      }
      if (dx < vbs.d.size ()) {
	out[elements].key = curkey;
	out[elements].auxdata = vbs.d[dx].auxdata;
	elements++;
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

  if (elements < limit) {
    out.setsize(elements);
  }

  (void) cursor->c_close (cursor);
  return r;
}
// }}}
// {{{ dbns::updateone
int
dbns::updateone (const chordID &key, const adb_bsinfo_t &bsinfo, bool present,
	         DB_TXN *t)
{
  int r;
  str key_str = id_to_str (key);
  DBT skey;
  str_to_dbt (key_str, &skey);
  DBT data; 
  bzero (&data, sizeof (data));
  adb_vbsinfo_t vbs;

  // get, with a write lock.
  r = bsdb->get (bsdb, t, &skey, &data, DB_RMW);
  if (r && r != DB_NOTFOUND) {
    warner ("dbns::update", "get error", r);
    return r;
  }
  // append/change
  if (r == DB_NOTFOUND) {
    if (present) {
      vbs.d.push_back ();
      vbs.d.back().n = bsinfo.n;
      vbs.d.back().auxdata = bsinfo.auxdata;
    } else {
      // Nothing to do, go home.
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
      else {
	vbs.d.push_back ();
	vbs.d.back().n = bsinfo.n;
	vbs.d.back().auxdata = bsinfo.auxdata;
      }
    } else {
      if (dx < vbs.d.size ()) {
	vbs.d[dx] = vbs.d.back ();
	vbs.d.pop_back ();
      } else {
	// No need to re-write to db; nothing changed.
	return 0;
      }
    }
  }
  // put
  str vbsout = xdr2str (vbs);
  str_to_dbt (vbsout, &data);
  r = bsdb->put (bsdb, t, &skey, &data, 0);
  if (r)
    warner ("dbns::update", "put error", r);
  return r;
}
// }}}
// {{{ dbns::update
int
dbns::update (const chordID &key, const adb_bsinfo_t &bsinfo, bool present)
{
  int r;
  DB_TXN *t = NULL;
  dbns_txn_begin (dbe, &t);

  r = updateone (key, bsinfo, present, t);
  if (r)
    dbns_txn_abort (dbe, t);
  else
    dbns_txn_commit (dbe, t);
  return r;
}
// }}}
// {{{ dbns::updatebatch
int
dbns::updatebatch (rpc_vec<adb_updatearg, RPC_INFINITY> &uargs)
{
  int r (0);
  DB_TXN *t = NULL;
  dbns_txn_begin (dbe, &t);
  for (size_t i = 0; i < uargs.size (); i++) {
    r = updateone (uargs[i].key, uargs[i].bsinfo, uargs[i].present, t);
    if (r) {
      dbns_txn_abort (dbe, t);
      break;
    }
  }
  if (!r) {
    dbns_txn_commit (dbe, t);
  }
  return r;
}
// }}}
// {{{ dbns::getinfo
int
dbns::getinfo (const chordID &key, rpc_vec<adb_vnodeid, RPC_INFINITY> &out)
{
  int r;
  str key_str = id_to_str (key);
  DBT skey;
  str_to_dbt (key_str, &skey);
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
  if (p[p.len () - 1] != '/')
    dbpath = strbuf () << p << "/";

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

  // implicit policy: don't insert the same key twice for non-aux dbs.
  bool inserted_new_key = db->kinsert (arg->key, arg->auxdata);
  if (!inserted_new_key) {
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
  if (db->hasaux () || arg->ordered) {
    r = db->getkeys (arg->continuation, arg->batchsize, arg->getaux, res.resok->keyaux);
    if (!r)
      res.resok->continuation = incID (res.resok->keyaux.back ().key);
  } else {
    u_int32_t start = arg->continuation.getui ();
    r = db->kgetkeys (start, arg->batchsize, arg->getaux, res.resok->keyaux);
    if (!r)
      res.resok->continuation = bigint (start + res.resok->keyaux.size ());
  }
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
// {{{ do_getblockrange
void
do_getblockrange (dbmanager *dbm, svccb *sbp)
{

  adb_getblockrangearg *arg = sbp->Xtmpl getarg<adb_getblockrangearg> ();
  adb_getblockrangeres *res = sbp->Xtmpl getres<adb_getblockrangeres> ();
  dbns *db = dbm->get (arg->name);

  if (!db) {
    res->status = ADB_ERR;
    sbp->reply (res);
    return;
  }

  int r (0);
  res->status = ADB_OK;
  if (arg->extant >= 0) {
    r = db->getblockrange_extant (arg->start, arg->stop,
	  arg->extant, arg->count, res->blocks);
  } else {
    r = db->getblockrange_all (arg->start, arg->stop,
	  arg->count, res->blocks);
  }
  if (r) {
    if (r == DB_NOTFOUND) {
      res->status = ADB_COMPLETE;
    } else {
      res->status = ADB_ERR;
      res->blocks.clear ();
    }
  }

  sbp->reply (res);

}
// }}}
// {{{ do_getkeyson
void
do_getkeyson (dbmanager *dbm, svccb *sbp)
{
  adb_getkeysonarg *arg = sbp->Xtmpl getarg<adb_getkeysonarg> ();
  adb_getkeysres *res = sbp->Xtmpl getres<adb_getkeysres> ();
  res->set_status (ADB_OK);
  dbns *db = dbm->get (arg->name);

  if (!db) {
    res->set_status (ADB_ERR);
    sbp->reply (res);
    return;
  }
  res->resok->hasaux = db->hasaux ();

  int r = db->getkeyson (arg->who, arg->start, arg->stop, 128, 
			 res->resok->keyaux);
  res->resok->complete = (r == DB_NOTFOUND);
  if (r && r != DB_NOTFOUND) 
    res->set_status (ADB_ERR);
  sbp->reply (res);

}
// }}}
// {{{ do_update
void
do_update (dbmanager *dbm, svccb *sbp)
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
// {{{ do_updatebatch
void
do_updatebatch (dbmanager *dbm, svccb *sbp)
{
  adb_updatebatcharg *args = sbp->Xtmpl getarg<adb_updatebatcharg> ();
  adb_status res (ADB_OK);
  if (!args->args.size ()) {
    sbp->replyref (res);
    return;
  }
  dbns *db = dbm->get (args->args[0].name);
  // assert: all args have the same namespace because batching
  //         is done by the adb object in libadb.C and each adb obj
  //         serves one namespace
  if (!db) {
    sbp->replyref (ADB_ERR);
    return;
  }
  int r = db->updatebatch (args->args);
  if (r)
    res = ADB_ERR;
  sbp->replyref (res);
}
// }}}
// {{{ do_getinfo
void
do_getinfo (dbmanager *dbm, svccb *sbp)
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
  case ADBPROC_GETBLOCKRANGE:
    do_getblockrange (dbm, sbp);
    break;
  case ADBPROC_GETKEYSON:
    do_getkeyson (dbm, sbp);
    break;
  case ADBPROC_UPDATE:
    do_update (dbm, sbp);
    break;
  case ADBPROC_UPDATEBATCH:
    do_updatebatch (dbm, sbp);
    break;
  case ADBPROC_GETINFO:
    do_getinfo (dbm, sbp);
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

  //setup the asrv
  listen_unix (dbsock, dbm);

  amain ();
}

// vim: foldmethod=marker
