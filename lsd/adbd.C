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

#include <sys/types.h>
#include <dirent.h>

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

// Run an expiration of the mtree every this many seconds.
static u_int32_t expire_mtree_interval (60);
// If an object will expire in this many seconds, ignore it.
static u_int32_t expire_buffer (15 * 60);

// How frequently to consider checkpointing the database.
// This affects the amount of disk I/O that's going to happen.
static u_int32_t sync_interval (60);
static u_int32_t max_unchkpt_log_size (1024); // KB

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
  // this flag should mean that berkeley db will free this memory when it's
  // done with it.
  skey->flags = DB_DBT_APPMALLOC;
  skey->data = expire;
  skey->size = sizeof (*expire);

  if (pkey->size == master_metadata.size && 
      !memcmp (pkey->data, master_metadata.data, pkey->size))
  {
    *expire = 0;
    return 0;
  }

  adb_metadata_t md;
  if (!buf2xdr (md, pdata->data, pdata->size)) {
    hexdump hd (pdata->data, pdata->size);
    warn << "getexpire: unable to unmarshal pdata.\n" << hd << "\n";
    return -1;
  }
  // Ensure big-endian for proper BDB sorting.
  *expire = htonl (md.expiration);
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

  str datapath;

  DB *metadatadb;
  DB *byexpiredb;

  u_int32_t last_mtree_time;
  merkle_tree_bdb *mtree;

  adb_master_metadata_t mmd;

  timecb_t *mtree_tcb;
  void mtree_cleaner ();
  timecb_t *sync_tcb;
  void periodic_sync ();

  int expire_walk (u_int32_t limit, u_int32_t start, u_int32_t end,
    vec<DBT> &victims, vec<adb_metadata_t> &victim_metadata);
  int update_metadata (bool add, u_int64_t sz, u_int32_t expiration, DB_TXN *t = NULL);

public:
  dbns (const str &dbpath, const str &name, bool aux, str logpath = NULL);
  ~dbns ();

  void warner (const char *method, const char *desc, int r);

  void sync (bool force = false);

  bool hasaux () { return aux; };
  str getname () { return name; }

  int get_metadata (const chordID &key, adb_metadata_t &metadata, DB_TXN *t = NULL);

  // Primary data management
  int insert (const chordID &key, DBT &data, u_int32_t auxdata = 0, u_int32_t exptime = 0);
  int lookup (const chordID &key, str &data, adb_metadata_t &md);
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
  int expire_mtree ();
  int expire (u_int32_t limit = 0, u_int32_t t = 0);

  str time2fn (u_int32_t exptime);
  int write_object (const chordID &key, DBT &data, u_int32_t t);
  int read_object (const chordID &key, str &data, adb_metadata_t &md);
  int expire_objects (u_int32_t exptime);
};
// }}}
// {{{ dbns::dbns
dbns::dbns (const str &dbpath, const str &name, bool aux, str logpath) :
  name (name),
  aux (aux),
  dbe (NULL),
  metadatadb (NULL),
  byexpiredb (NULL),
  last_mtree_time (0),
  mtree (NULL),
  mtree_tcb (NULL),
  sync_tcb (NULL)
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
  r = dbfe_initialize_dbenv (&dbe, fullpath, false, 10*1024, logconf);
  DBNS_ERRCHECK ("dbe->open");

  datapath = fullpath << "/data";
  mkdir (datapath, 0755);

  mtree = New merkle_tree_bdb (dbe, /* ro = */ false);

  r = dbfe_opendb (dbe, &metadatadb, "metadatadb", DB_CREATE, 0);
  DBNS_ERRCHECK ("metadatadb->open");
  r = dbfe_opendb (dbe, &byexpiredb, "byexpiredb", DB_CREATE, 0, /* dups = */ true);
  DBNS_ERRCHECK ("byexpiredb->open");
  r = metadatadb->associate (metadatadb, NULL, byexpiredb, getexpire, DB_AUTO_COMMIT);
  DBNS_ERRCHECK ("metadatdb->associate (byexpiredb)");

  mtree_cleaner ();
  periodic_sync ();

  warn << "dbns::dbns (" << dbpath << ", " << name << ", " << aux << ")\n";
}
// }}}
// {{{ dbns::~dbns
dbns::~dbns ()
{
  if (mtree_tcb) {
    timecb_remove (mtree_tcb);
    mtree_tcb = NULL;
  }
  if (sync_tcb) {
    timecb_remove (sync_tcb);
    sync_tcb = NULL;
  }
  sync (/* force = */ true);

#define DBNS_DBCLOSE(x)			\
  if (x) {				\
    (void) x->close (x, 0); x = NULL;	\
  }

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
// {{{ dbns::periodic_sync
void
dbns::periodic_sync ()
{
  sync_tcb = NULL;
  sync ();
  sync_tcb = delaycb (sync_interval + (tsnow.tv_nsec % 10),
      wrap (this, &dbns::periodic_sync));
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
  txn_checkpoint (dbe, max_unchkpt_log_size, 10, flags);
#else
  dbe->txn_checkpoint (dbe, max_unchkpt_log_size, 10, flags);
#endif
}
// }}}
// {{{ dbns::update_metadata (bool, u_int64_t, u_int32_t, DB_TXN *)
int
dbns::update_metadata (bool add, u_int64_t sz, u_int32_t expiration, DB_TXN *t)
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
  if (!add && mmd.size < sz) {
    fatal << "dbns::update_metadata: small size: "
          << mmd.size << " < " << sz << "\n";
  }
  if (add)
    mmd.size += sz;
  else
    mmd.size -= sz;
  if (add && quota && mmd.size > quota)
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
// {{{ dbns::get_metadata
int
dbns::get_metadata (const chordID &key, adb_metadata_t &metadata, DB_TXN *t)
{
  DBT skey;
  id_to_dbt (key, &skey);
  DBT md;
  bzero (&md, sizeof (md));
  int r = metadatadb->get (metadatadb, t, &skey, &md, 0);
  if (r) {
    if (r != DB_NOTFOUND)
      warner ("dbns::get_metadata", "metadatadb->get", r);
    return r;
  }
  if (!buf2xdr (metadata, md.data, md.size))
    return -1;
  return 0;
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

  adb_metadata_t oldmetadata;
  r = get_metadata (key, oldmetadata, t);
  if (r != DB_NOTFOUND) {
    dbfe_txn_abort (dbe, t);
    return r;
  }

  r = update_metadata (true, data.size, exptime, t);
  if (r) {
    dbfe_txn_abort (dbe, t);
    // Even if r == ENOSPC, we can't afford to blow a lot of time
    // here doing expiration.
    return r;
  }

  // Prep BDB objects.
  DBT skey;
  id_to_dbt (key, &skey);

  const char *err = "";
  int ret;
  if (exptime > timenow + expire_buffer) {
    // Only add to Merkle tree if this object is worth repairing.
    if (hasaux ()) {
      err = "mtree->insert aux";
      r = mtree->insert (key, auxdata, t);
    } else {
      err = "mtree->insert";
      r = mtree->insert (key, t);
      // insert may return DB_KEYEXIST in which case we need
      // not do any more work here.
    }
    if (r) {
      if (r != DB_KEYEXIST)
	warner ("dbns::insert", err, r);
      ret = dbfe_txn_abort (dbe, t);
      if (ret)
	warner ("dbns::insert", "abort/commit error", ret);
      return r;
    }
  }

  u_int32_t offset = write_object (key, data, exptime);
  if (offset < 0) {
    int saved_errno = errno;
    warn ("dbns::insert: write_object failed: %m\n");
    ret = dbfe_txn_abort (dbe, t);
    if (ret)
      warner ("dbns::insert", "abort/commit error", ret);
    return saved_errno;
  }

  adb_metadata_t md;
  md.size = data.size;
  md.auxdata = auxdata;
  md.expiration = exptime;
  md.offset = offset;

  str md_str = xdr2str (md);
  DBT metadata;
  str_to_dbt (md_str, &metadata);
  err = "metadatadb->put";
  r = metadatadb->put (metadatadb, t, &skey, &metadata, 0);
  if (r) {
    assert (r != DB_KEYEXIST); // Already checked.
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
dbns::lookup (const chordID &key, str &data, adb_metadata_t &md)
{
  int r = 0;

  r = read_object (key, data, md);
  // read_object returns -1 on error, 0 otherwise.
  if (r) {
    // Treat all errors as not found.
    return DB_NOTFOUND;
  }
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
  r = metadatadb->cursor (metadatadb, NULL, &cursor, 0);

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
  // NB! This does not remove any actual data.
  int r = 0;
  adb_metadata_t metadata;
  r = get_metadata (key, metadata);
  if (r)
    return r;

  DBT skey;
  id_to_dbt (key, &skey);

  DB_TXN *t = NULL;
  r = dbfe_txn_begin (dbe, &t);
  assert (r == 0);

  // Update min expiration lazily; we can't know if this is the
  // last object with a particular expiration time.
  r = update_metadata (false, metadata.size, 0, t);
  if (r) {
    dbfe_txn_abort (dbe, t);
    return r;
  }

  r = metadatadb->del (metadatadb, t, &skey, 0);
  if (r) {
    if (r != DB_NOTFOUND)
      warner ("dbns::remove", "metadatadb->del", r);
    dbfe_txn_abort (dbe, t);
    return r;
  }

  // Only attempt to update Merkle tree if object was present.
  const char *err = "";
  if (hasaux ()) {
    err = "mtree->remove aux";
    r = mtree->remove (key, auxdata, t);
  } else {
    err = "mtree->remove";
    r = mtree->remove (key, t);
  }
  int ret = 0;
  if (r && r != DB_NOTFOUND) {
    warner ("dbns::remove", err, r);
    ret = dbfe_txn_abort (dbe, t);
  } else {
    // Ignore any NOTFOUND errors since the Merkle
    // key may have been removed by expiration.
    r = 0;
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
// {{{ dbns::expire_walk
// Grab limit keys from time start to end into victims/victim_metadata.
// Caller is responsible for freeing the data allocated into
// the victims DBTs.
int
dbns::expire_walk (u_int32_t limit, u_int32_t start, u_int32_t end,
    vec<DBT> &victims, vec<adb_metadata_t> &victim_metadata)
{
  // Open a cursor in secondary database.
  // Make sure it points to the first thing after 0.
  // Keep reading and deleting until the thing's key is > t.
  //   Accumulate entries into a vec, including the object size.
  u_int32_t begin_time_data = htonl (start);
  DBT begin_time; bzero (&begin_time, sizeof (begin_time));
  begin_time.data = &begin_time_data;
  begin_time.size = sizeof (begin_time_data);
  DBT key; bzero (&key, sizeof (key));
  key.flags = DB_DBT_MALLOC;
  DBT content; bzero (&content, sizeof (content));
  DBC *cursor = NULL;
  int r = byexpiredb->cursor (byexpiredb, NULL, &cursor, 0);
  if (r) {
    warner ("dbns::expire_walk", "byexpiredb->cursor", r);
    if (cursor)
      cursor->c_close (cursor);
    return r;
  }

  r = cursor->c_pget (cursor, &begin_time, &key, &content, DB_SET_RANGE);
  while (!r) {
    if (key.size == master_metadata.size && 
	!memcmp (key.data, master_metadata.data, key.size))
    {
      free (key.data);
    } else if (limit > 0 && victims.size () >= limit) {
      free (key.data);
      break;
    } else {
      adb_metadata_t md;
      buf2xdr (md, content.data, content.size);
      if (md.expiration < end) {
	victims.push_back (key);
	victim_metadata.push_back (md);
      } else {
	free (key.data);
	break;
      }
    }
    bzero (&key, sizeof (key));
    key.flags = DB_DBT_MALLOC;
    r = cursor->c_pget (cursor, &begin_time, &key, &content, DB_NEXT);
  }
  if (r && r != DB_NOTFOUND)
    warner ("dbns::expire_walk", "byexpiredb cursor->c_pget", r);
  (void) cursor->c_close (cursor);
  return r;
}
// }}}
// {{{ dbns::mtree_cleaner
void
dbns::mtree_cleaner ()
{
  expire_mtree ();
  // Align the next run time so that everyone in the system runs
  // at approximately the start of the next interval, with some jitter.
  // This should reduce the number of spurious sync repairs.
  u_int32_t next_interval =
    (tsnow.tv_sec / expire_mtree_interval) * expire_mtree_interval +
      expire_mtree_interval + (tsnow.tv_nsec % 10);
  mtree_tcb = delaycb (next_interval - tsnow.tv_sec,
      wrap (this, &dbns::mtree_cleaner));
}
// }}}
// {{{ dbns::expire_mtree
// Clean out expired keys from the local Merkle tree
int
dbns::expire_mtree ()
{
  vec<DBT> victims;
  vec<adb_metadata_t> victim_metadata;
  u_int32_t now = time (NULL);
  // Round to the next lowest expire_mtree_interval.
  // This may compensate for the jitter scheduled in by mtree_cleaner.
  now = (now / expire_mtree_interval) * expire_mtree_interval;

  // Get all objects from the last time we did this until present
  int r = expire_walk (0, last_mtree_time, now, victims, victim_metadata);

  DB_TXN *t = NULL;
  int retry_count = 0;
  // Iterate over objects to be expired:
  while (victims.size ()) {
    retry_count = 0;
retry:
    t = NULL;
    dbe->txn_begin (dbe, NULL, &t, 0);
    DBT key = victims.pop_back ();
    adb_metadata_t md = victim_metadata.pop_back ();
    chordID id = dbt_to_id (key);
    if (hasaux ()) {
      r = mtree->remove (id, md.auxdata, t);
    } else {
      r = mtree->remove (id, t);
    }
    switch (r) {
      case 0:
	warnx ("%d.%06d ", int (tsnow.tv_sec), int (tsnow.tv_nsec/1000))
	  << name << ": Expired mtree " << id << "\n";
	dbfe_txn_commit (dbe, t);
	break;
      case DB_NOTFOUND:
	// Okay to continue
	dbfe_txn_abort (dbe, t);
	break;
      case DB_LOCK_DEADLOCK:
	// Must immediately abort.
	dbfe_txn_abort (dbe, t);
	warner ("dbns::expire_mtree", "mtree remove", r);
	if (retry_count < 10) {
	  retry_count++;
	  victims.push_back (key);
	  victim_metadata.push_back (md);
	  goto retry;
	} else {
	  // Give up on the whole thing.
	  warnx << name << ": too many retries for " << id
	        << "; aborting.\n";
	  free (key.data);
	  goto abort_cleanup;
	}
	break;
      default:
	warner ("dbns::expire_mtree", "mtree remove", r);
	dbfe_txn_abort (dbe, t);
	goto abort_cleanup;
	break;
    }
    free (key.data);
  }

  last_mtree_time = now;
  return r;

abort_cleanup:
  while (victims.size ()) {
    DBT key = victims.pop_back ();
    free (key.data);
  }
  // Leave last_mtree_time as old, make sure we get everything
  // that we should have done this round.
  return r;
}
// }}}
// {{{ dbns::expire (u_int32_t, u_int32_t)
int
dbns::expire (u_int32_t limit, u_int32_t deadline)
{
  if (deadline == 0)
    deadline = time (NULL);

  vec<DBT> victims;
  vec<adb_metadata_t> victim_metadata;
  int r = expire_walk (limit, 1, deadline, victims, victim_metadata);

  u_int64_t victim_size = 0;
  u_int32_t last_expire = 0;
  // XXX Would it be okay to modify the database (and its
  //     sibling databases) while the cursor is open?

  // start a transaction
  DB_TXN *parent = NULL;
  dbfe_txn_begin (dbe, &parent);
  u_int32_t txnsize = 0;
  // Iterate over objects to be expired:
  while (victims.size ()) {
    DB_TXN *t = NULL;
    dbe->txn_begin (dbe, parent, &t, 0);
    DBT key = victims.pop_back ();
    adb_metadata_t md = victim_metadata.pop_back ();
    chordID id = dbt_to_id (key);
    warnx ("%d.%06d ", int (tsnow.tv_sec), int (tsnow.tv_nsec/1000))
      << name << ": Expiring " << id << "\n";
    const char *err = "";
    do {
      err = "mtree->remove";
      if (hasaux ()) {
	r = mtree->remove (id, md.auxdata, t);
      } else {
	r = mtree->remove (id, t);
      }
      // Ignore error on mtree removals
      err = "metadatadb->del";
      r = metadatadb->del (metadatadb, t, &key, 0);
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

    txnsize++;
    if (txnsize > 1000) {
      txnsize = 0;
      r = update_metadata (false, victim_size, last_expire, parent);
      victim_size = 0;
      dbfe_txn_commit (dbe, parent);
      dbfe_txn_begin (dbe, &parent);
    }
  }
  // Update the metadata with size/time difference
  r = update_metadata (false, victim_size, last_expire, parent);
  dbfe_txn_commit (dbe, parent);

  // Metadata is gone, now remove data.
  expire_objects (last_expire);

  return r;
}
// }}}
// {{{ dbns::time2fn
str
dbns::time2fn (u_int32_t exptime)
{
  // Each bin holds 256 seconds worth of writes.
  // The Dell PowerEdge SC1425s with Maxtor 6Y160M0 SATA disks 
  // write at 6MB/s with write caching disabled, for about 1.5GB files.
  // The name of the file is the expiration time rounded up to the nearest
  // multiple of 0xFF so the filename's time has past, the file can be unlinked.
  static char subpath[20];
  sprintf (subpath, "%04x/%08x",
      ((exptime & 0xFFFF0000) >> 16), ((exptime + 0xFF) & 0xFFFFFF00));
  str path = datapath << "/" << subpath; 
  return path;
}
// }}}
// {{{ dbns::write_object
void
mkpath (const char *path)
{
  int len = strlen (path);
  char *buf = New char[len];
  const char *slash = path;

  while ((slash = strchr(slash+1, '/')) != NULL) {
    len = slash - path;
    memcpy(buf, path, len);
    buf[len] = 0;
    if (mkdir(buf, 0777)) {
      if (errno == EEXIST) {
	struct stat st;
	if (!stat(buf, &st) && S_ISDIR(st.st_mode))
	  continue;
      }
      fatal ("mkpath at %s: %m", buf);
    }
  }
  delete[] buf;
}

int
dbns::write_object (const chordID &key, DBT &data, u_int32_t exptime)
{
  // Cache file descriptors for commonly used files.
  str fn = time2fn (exptime);

  mkpath (fn);
  int fd = open (fn, O_CREAT|O_WRONLY|O_APPEND, 0666);
  if (fd < 0)
    return fd;
  struct stat sb;
  if (fstat (fd, &sb) < 0)
    return -1;
  if (write (fd, data.data, data.size) != (int) data.size) {
    int saved_errno = errno;
    close (fd);
    errno = saved_errno;
    return -1;
  }
  close (fd);
  return sb.st_size;
}
// }}}
// {{{ dbns::read_object
int
dbns::read_object (const chordID &key, str &data, adb_metadata_t &metadata)
{
  // Get the metadata necessary to do the read.
  int r = get_metadata (key, metadata);
  if (r) {
    if (r != DB_NOTFOUND)
      warner ("dbns::read_object", "get_metadata", r);
    return -1;
  }

  str fn = time2fn (metadata.expiration);
  int fd = open (fn, O_RDONLY);
  if (fd < 0) {
    if (errno != ENOENT)
      warn ("open: %m\n");
    return -1;
  }
  if (lseek (fd, metadata.offset, SEEK_SET) < 0) {
    warn ("lseek: %m\n");
    close (fd);
    return -1;
  }
  mstr raw (metadata.size);
  char *buf = raw.cstr ();
  u_int32_t left = metadata.size;
  while (left > 0) {
    int nread = read (fd, buf, left);
    if (nread < 0) {
      warn ("read: %m\n");
      break;
    } else if (nread == 0) {
      warn << "EOF reading " << key << " from " << fn << "\n";
      break;
    } else {
      left -= nread;
      buf  += nread;
    }
  }
  close (fd);
  if (left == 0) {
    data = raw;
    return 0;
  }
  return -1;
}
// }}}
// {{{ dbns::expire_objects
int
dbns::expire_objects (u_int32_t exptime)
{
  u_int32_t hightime = exptime >> 16;
  DIR *datadir = opendir (datapath);
  if (!datadir) {
    warn ("opendir: %s %m\n", datapath.cstr ());
    return -1;
  }
  struct dirent *dp = NULL;
  // This code relies on file names making sense according to time2fn.
  // If it were a little more suspicious, it might stat the files.
  while ((dp = readdir (datadir)) != NULL) {
    if (strlen (dp->d_name) != 4)
      continue;
    char *ep = NULL;
    u_int32_t t = strtoul (dp->d_name, &ep, 16);
    if (!ep || *ep != '\0')
      continue;
    assert ((t & 0xFFFF0000) == 0);
    if (t > 0 && t <= hightime) {
      str subdirpath = datapath << "/" << dp->d_name;
      DIR *subdir = opendir (subdirpath);
      if (!subdir) {
	warn ("opendir: %s: %m\n", subdirpath.cstr ());
	continue;
      }
      struct dirent *sdp = NULL;
      while ((sdp = readdir (subdir)) != NULL) {
	if (strlen (sdp->d_name) != 8)
	  continue;
	char *ep = NULL;
	u_int32_t rt = strtoul (sdp->d_name, &ep, 16);
	if (!ep || *ep != '\0')
	  continue;
	if (rt < exptime) {
	  str filepath = subdirpath << "/" << sdp->d_name;
	  if (unlink (filepath) < 0)
	    warn ("unlink: %s: %m\n", filepath.cstr ());
	}
      }
      closedir (subdir);
      rmdir (subdirpath);
    }
  }
  closedir (datadir);
  return 0;
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
  if (logpath) {
    mkdir_wrapper (logpath);
    logpath = canonicalize_path (logpath);
  }

  // Add trailing slash if necessary
  str x = canonicalize_path (dbpath);
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
    case DB_KEYEXIST:
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
  adb_metadata_t md;
  bzero (&md, sizeof (md));

  int r;
  chordID key;
  if (arg->nextkey) {
    r = db->lookup_nextkey (arg->key, key);
    data = "";
  } else {
    r = db->lookup (arg->key, data, md);
    key = arg->key;
  }

  if (r) {
    res.set_status ((r == DB_NOTFOUND ? ADB_NOTFOUND : ADB_ERR));
  } else {
    res.resok->key = key;
    res.resok->data = data;
    res.resok->expiration = md.expiration;
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
    a->setcb (NULL);
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
