/*
 * dbfe.C
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#include "dbfe.h"

#ifdef DMALLOC
#include "dmalloc.h"
#endif

//adb options
#define CACHE_OPT "opt_cachesize"
#define NODESIZE_OPT "opt_nodesize"
#define LEAFSIZE_OPT "opt_leafsize"
#define ASYNC_OPT "opt_async"

//sleepy options
#define PERM_OPT "opt_permissions"
#define TYPE_OPT "opt_dbtype"
#define CREATE_OPT "opt_create"
#define FLAG_OPT   "opt_flag"
#define JOIN_OPT  "opt_join"
#define DBENV_OPT    "opt_dbenv"

///////////////// static /////////////////////

// XXX fix the u_int casting..

ref<dbImplInfo>
dbGetImplInfo() {
  ref<dbImplInfo> info = New refcounted<dbImplInfo>();
#ifndef SLEEPYCAT
  info->supportedOptions.push_back(CACHE_OPT);
  info->supportedOptions.push_back(NODESIZE_OPT);
  info->supportedOptions.push_back(ASYNC_OPT);
  info->supportedOptions.push_back(CREATE_OPT);
#else
  info->supportedOptions.push_back(CACHE_OPT);
  info->supportedOptions.push_back(PERM_OPT);
  info->supportedOptions.push_back(TYPE_OPT);
  info->supportedOptions.push_back(CREATE_OPT);
  info->supportedOptions.push_back(FLAG_OPT);
  info->supportedOptions.push_back(JOIN_OPT);
  info->supportedOptions.push_back(DBENV_OPT);
#endif
  return info;
}

//////////////////// dbOptions ///////////////////////
dbOptions::dbOptions()  {}

int verifyOption(char *optionSig) {
  vec<char *> allowed = dbGetImplInfo()->supportedOptions;
  for (unsigned int i=0; i < allowed.size(); i++) 
    if (memcmp(allowed[i], optionSig, strlen(allowed[i])) == 0) return 1;
  return 0;
}

int
dbOptions::addOption(char *optionSig, long value) {
  if (!verifyOption(optionSig)) return EINVAL;

  optionRec optr;
  optr.sig = optionSig;
  optr.value = value;
  options.push_back(optr);
  return 0;
}

long
dbOptions::getOption(char *optionSig) {
  for (unsigned int i=0; i < options.size(); i++) 
    if (memcmp(options[i].sig, optionSig, strlen(options[i].sig)) == 0) return options[i].value;
  return -1;
}


//////////////////////dbEnumeration//////////////////////

#ifdef SLEEPYCAT
dbEnumeration::dbEnumeration(DB *db, DB_ENV *dbe) {
  int r = 0;
  db_sync = db;

  r = db->cursor(db, NULL, &cursor, 0);
  if (r) {
    const char *path (NULL);
    dbe->get_home (dbe, &path);
    fatal << "enumeration error for " << path << ": "
          << db_strerror (r) << "\n";
  }
  cursor_init = 0;
}

dbEnumeration::~dbEnumeration() {
  cursor->c_close (cursor);
}

#else
dbEnumeration::dbEnumeration(btreeSync *adb) {

  it = New bIteration();
  ADB_sync = adb;
  ADB_async = NULL;
}
dbEnumeration::dbEnumeration(btreeDispatch *adb) {
  it = New bIteration();
  ADB_async = adb;
  ADB_sync = NULL;
}
dbEnumeration::~dbEnumeration() {
  delete it;
}
#endif

char
dbEnumeration::hasMoreElements()
{
#ifdef SLEEPYCAT
  assert (0); // i don't think this works.  --josh

  char ci = cursor_init;
  ptr<dbPair> next = nextElement();
  if (next == NULL) return 0;
 
  //otherwise, back up one in preparation for the call
  if (!ci) {
    //reset the cursor to beginning
    db_sync->cursor(db_sync, NULL, &cursor, 0);
    return 1;
  }
  DBT key, data;
  bzero(&key, sizeof(key));
  bzero(&data, sizeof(data));
  cursor->c_get(cursor, &key, &data, DB_PREV);
  return 1;
#else
  return (it->lastNode() != 0);
#endif
}

#ifdef SLEEPYCAT
ptr<dbPair>
dbEnumeration::getElement(u_int32_t flags, ptr<dbrec> startkey)
{
  // XXX hack
  // nextElement doesn't return any data, just keys.
  // use the lookup routine if you want the data. 
  // Perhaps, change nextElement to only return a ptr<dbrec>!
  DBT key;
  bzero(&key, sizeof(key));
  if (startkey) {
    key.size = startkey->len;
    key.data = startkey->value;
  }

  DBT data;
  bzero(&data, sizeof(data));
  data.flags = DB_DBT_PARTIAL;

  int err = cursor->c_get(cursor, &key, &data, flags);
  cursor_init = 1;
  if (err) {
    //    warn << "db3 error: " << db_strerror(err) << "\n";
    return NULL;
  }
  ref<dbrec> keyrec = New refcounted<dbrec>(key.data, key.size);
  ptr<dbrec> valrec = NULL;
  return New refcounted<dbPair>(keyrec, valrec);
}

ptr<dbPair>
dbEnumeration::nextElement()
{
  return getElement (DB_NEXT, NULL);
}

ptr<dbPair>
dbEnumeration::prevElement()
{
  return getElement (DB_PREV, NULL);
}

ptr<dbPair>
dbEnumeration::nextElement(ref<dbrec> startkey)
{
  return getElement(DB_SET_RANGE, startkey);
}

ptr<dbPair>
dbEnumeration::lastElement()
{
  return getElement(DB_LAST, NULL);
}

ptr<dbPair>
dbEnumeration::firstElement()
{
  return getElement(DB_FIRST, NULL);
}

#else
ptr<dbPair> dbEnumeration::nextElement() {
  assert(ADB_sync);
  
  record *rec;
  int err = ADB_sync->iterate(it, &rec);
  if (err) return NULL;
  void *val, *key;
  long valLen, keyLen;
  val = rec->getValue(&valLen);
  key = rec->getKey(&keyLen);
  
  ref<dbrec> keyrec = New refcounted<dbrec>(key, keyLen);
  ref<dbrec> valrec = New refcounted<dbrec>(val, valLen);
  
  return New refcounted<dbPair>(keyrec, valrec);
  
}

void dbEnumeration::nextElement(callback<void, ptr<dbPair> >::ref cb) {
  assert(ADB_async);
  ADB_async->iterate(it, wrap(this, &dbEnumeration::ne_cb, cb));
  return;
}

void dbEnumeration::ne_cb(callback<void, ptr<dbPair> >::ref cb, 
			   tid_t tid, 
			   int err, 
			   record *rec) {
 
  if ((err) || (rec == NULL)) { 
    (*cb)(NULL);
    return;
  }

  void *val, *key;
  long valLen, keyLen;
  val = rec->getValue(&valLen);
  key = rec->getKey(&keyLen);
  
  ref<dbrec> keyrec = New refcounted<dbrec>(key, keyLen);
  ref<dbrec> valrec = New refcounted<dbrec>(val, valLen);
  
  (*cb)(New refcounted<dbPair>(keyrec, valrec));
  
}
#endif
  
////////////////////// dbfe //////////////////////////////
dbfe::dbfe() {
  
#ifndef SLEEPYCAT
    create_impl = wrap(this, &dbfe::IMPL_create_adb);
    open_impl = wrap(this, &dbfe::IMPL_open_adb);
    close_impl = wrap(this, &dbfe::IMPL_close_adb);

    insert_impl = wrap(this, &dbfe::IMPL_insert_sync_adb);
    lookup_impl = wrap(this, &dbfe::IMPL_lookup_sync_adb);
    
    insert_impl_async = wrap(this, &dbfe::IMPL_insert_async_adb);
    lookup_impl_async = wrap(this, &dbfe::IMPL_lookup_async_adb);
    make_enumeration = wrap(this, &dbfe::IMPL_make_enumeration_adb);

    gADB_sync = NULL;
    gADB_async = NULL;
#else
    db = NULL;
    dbe = NULL;

    checkpoint_impl = wrap (this, &dbfe::IMPL_checkpoint_sleepycat);

    create_impl = wrap(this, &dbfe::IMPL_create_sleepycat);
    open_impl = wrap(this, &dbfe::IMPL_open_sleepycat);
    close_impl = wrap(this, &dbfe::IMPL_close_sleepycat);

    insert_impl = wrap(this, &dbfe::IMPL_insert_sync_sleepycat);
    lookup_impl = wrap(this, &dbfe::IMPL_lookup_sync_sleepycat);
    
    delete_impl = wrap(this, &dbfe::IMPL_delete_sync_sleepycat);
    delete_impl_async = wrap(this, &dbfe::IMPL_delete_async_sleepycat);

    insert_impl_async = wrap(this, &dbfe::IMPL_insert_async_sleepycat);
    lookup_impl_async = wrap(this, &dbfe::IMPL_lookup_async_sleepycat);
    make_enumeration = wrap(this, &dbfe::IMPL_make_enumeration_sleepycat);
#endif

}

dbfe::~dbfe() {
  closedb ();
}

#ifdef SLEEPYCAT
int dbfe::IMPL_open_sleepycat(char *filename, dbOptions opts) { 
  int r = -1;
  bool do_dbenv = false;

  long use_dbenv = opts.getOption(DBENV_OPT);
  if (filename && use_dbenv) do_dbenv = true;

  long mode = opts.getOption(PERM_OPT);
  if (mode == -1) mode = 0664;

  long flags = opts.getOption(FLAG_OPT);
  if (flags == -1) flags = DB_CREATE;

  long join = opts.getOption(JOIN_OPT);

  long create = opts.getOption(CREATE_OPT);
  if (create == 0) flags |= DB_EXCL;

  long cachesize = opts.getOption(CACHE_OPT);
  if (cachesize == -1) cachesize = 1024;  /* KB */

  if (do_dbenv) {
    r = dbfe_initialize_dbenv (&dbe, filename, join >= 0, cachesize);
    if (r){
      warn << "dbe->open returned " << r << " which is " << db_strerror(r) << "\n";
      return r;
    }
    r = dbfe_opendb (dbe, &db, "db", flags, mode);
  } else {
    r = dbfe_opendb (dbe, &db, (const char *) filename, flags, mode);
  }
  return r;
}


void
dbfe::IMPL_checkpoint_sleepycat ()
{
  if (dbe)
#if (DB_VERSION_MAJOR < 4)
     txn_checkpoint (dbe, 0, 0, 0);
#else
     dbe->txn_checkpoint (dbe, 0, 0, 0);
#endif
}

int 
dbfe::IMPL_create_sleepycat(char *filename, dbOptions opts) 
{ 
  warn << "use open instead\n";
  return 0;
} 

int dbfe::IMPL_insert_sync_sleepycat(ref<dbrec> key, ref<dbrec> data) { 
  DB_TXN *t = NULL;
  int r = 0, tr = 0;
  DBT skey, content;

  bzero(&skey, sizeof(skey));
  bzero(&content, sizeof(content));
  content.size = data->len;
  content.data = data->value;
  skey.size = key->len;
  skey.data = key->value;

  if(dbe) {
#if DB_VERSION_MAJOR >= 4
    r = dbe->txn_begin(dbe, NULL, &t, 0);
#else
    r = txn_begin(dbe, NULL, &t, 0);
#endif
    if (r) return r;
  }

  r = db->put(db, t, &skey, &content, 0);

  if (r) {
    warn << "insert (put): db error: " << db_strerror(r) << "\n";
    return r;
  }

  if(t) {
#if DB_VERSION_MAJOR >= 4
    tr = t->commit(t, 0);
#else
    tr = txn_commit(t, 0);
#endif
    if (!r) r = tr;
    t = NULL;
  }

  if (r) warn << "insert: db error: " << db_strerror(r) << "\n";
  
  return r;
} 

ptr<dbrec> 
dbfe::IMPL_lookup_sync_sleepycat(ref<dbrec> key) 
{ 
  DBT skey, content;
  bzero(&skey, sizeof(skey));
  skey.size = key->len;
  skey.data = key->value;
  bzero(&content, sizeof(content));
  int r=0;

  
  if ((r = db->get(db, NULL, &skey, &content, 0)) != 0) return NULL;
  ptr<dbrec> ret = New refcounted<dbrec>(content.data, content.size);

  // warnx << "return " << content.size << "\n";

  return ret;
} 

int 
dbfe::IMPL_delete_sync_sleepycat(ptr<dbrec> key) {
  DB_TXN *t = NULL;
  int err, terr;
  DBT dkey;
  bzero(&dkey, sizeof(dkey));
  dkey.size = key->len;
  dkey.data = key->value;

  if(dbe) {
#if DB_VERSION_MAJOR >= 4
    err = dbe->txn_begin(dbe, NULL, &t, 0);
#else
    err = txn_begin(dbe, NULL, &t, 0);
#endif
    if (err) return err;
  }

  err = db->del (db, t, &dkey, 0);

  if(t) {
#if DB_VERSION_MAJOR >= 4
    terr = t->commit(t, 0);
#else
    terr = txn_commit (t, 0);
#endif
    if (!err) err = terr;
    t = NULL;
  }

  return err;
}

void dbfe::itemReturn_dummy_cb (itemReturn_cb cb, ptr<dbrec> ret)
{ 
  cb (ret); 
} 

void dbfe::errReturn_dummy_cb (errReturn_cb cb, int err)
{
  cb (err);
}

void dbfe::IMPL_insert_async_sleepycat
(ref<dbrec> key, ref<dbrec> data, errReturn_cb cb)
{
  int err = IMPL_insert_sync_sleepycat(key, data);
  timecb (tsnow, wrap(&dbfe::errReturn_dummy_cb, cb, err));
}

void dbfe::IMPL_lookup_async_sleepycat(ref<dbrec> key, itemReturn_cb cb)
{ 
  ptr<dbrec> ret = IMPL_lookup_sync_sleepycat(key);
  timecb (tsnow, wrap(&dbfe::itemReturn_dummy_cb, cb, ret));
}

int 
dbfe::IMPL_close_sleepycat() { 
  int r;
  r = db->close(db, 0);
  if(dbe) dbe->close(dbe, 0);
  return r;
}

ptr<dbEnumeration> dbfe::IMPL_make_enumeration_sleepycat()
{
  return New refcounted<dbEnumeration>(db, dbe);
}

void 
dbfe::IMPL_delete_async_sleepycat(ptr<dbrec> key, errReturn_cb cb)
{
  int err = IMPL_delete_sync_sleepycat(key);
  timecb (tsnow, wrap(&dbfe::errReturn_dummy_cb, cb, err));
}

void
dbfe::IMPL_sync () 
{
  if (dbe)
    return;
  db->sync (db, 0L);
}

#else 

ptr<dbEnumeration> dbfe::IMPL_make_enumeration_adb() {
  
  if (gADB_sync)
    return New refcounted<dbEnumeration>(gADB_sync);
  else
    return New refcounted<dbEnumeration>(gADB_async);
}

int
dbfe::IMPL_create_adb(char *filename, dbOptions opts) {
 
  long ns = opts.getOption(NODESIZE_OPT);
  if (ns == -1) ns = 4096;
  long dlf = opts.getOption(LEAFSIZE_OPT);
  if (dlf == -1) dlf = 4;
  long create = opts.getOption(CREATE_OPT);
  if (create == -1) create = 0;

  gADB_sync = NULL;
  gADB_async = NULL;

  return createTree(filename, create, ns, dlf);
}

int
dbfe::IMPL_open_adb(char *filename, dbOptions opts) {

  long cacheSize = opts.getOption(CACHE_OPT);
  if (cacheSize == -1) cacheSize = 1000000;
  long async = opts.getOption(ASYNC_OPT);
  if (async == -1) async = 0;
  warn << cacheSize << async;
  if (async) {
    gADB_async = New btreeDispatch(filename, cacheSize);
    if (gADB_async) return 0;
    else return -1;  
  } else {
    gADB_sync = New btreeSync();
    return gADB_sync->open(filename, cacheSize);
  }

}

int dbfe::IMPL_insert_sync_adb(ref<dbrec> key, ref<dbrec> data) { 
  assert(gADB_sync);
  return  gADB_sync->insert(key->value, key->len, data->value, data->len);
} 

ptr<dbrec> dbfe::IMPL_lookup_sync_adb(ref<dbrec> key) { 
  assert(gADB_sync);
  record *rec;
  void *retValue;
  long retLen;

  int err = gADB_sync->lookup(key->value, key->len, &rec);
  if (err) return NULL;
  
  retValue = rec->getValue(&retLen);
  ptr<dbrec> ret = New refcounted<dbrec>(retValue, retLen);
  return ret;
  
} 

void 
dbfe::IMPL_insert_async_adb(ref<dbrec> key, ref<dbrec> data, errReturn_cb cb)  { 
  assert(gADB_async);
  gADB_async->insert(key->value, key->len, data->value, data->len, wrap(this, &dbfe::IMPL_insert_async_adb_comp, cb));
}
void 
dbfe::IMPL_insert_async_adb_comp(errReturn_cb cb, tid_t tid, int err, record *res) {
  (*cb)(err);
}

void dbfe::IMPL_lookup_async_adb(ref<dbrec> key, itemReturn_cb cb)  { 
  assert(gADB_async);
  gADB_async->lookup(key->value, key->len, wrap(this, &dbfe::IMPL_lookup_async_adb_comp, cb));
}
void dbfe::IMPL_lookup_async_adb_comp(itemReturn_cb cb, tid_t tid, int err,record *res) {
  void *val;
  long len;

  if ((err) || (res == NULL)) { (*cb)(NULL); return; }
  
  val = res->getValue(&len);

  ref<dbrec> ret = New refcounted<dbrec>(val, len);
  (*cb)(ret);
}

int dbfe::IMPL_close_adb() { 
  if (gADB_async){
    gADB_async->finalize(wrap(this, &dbfe::IMPL_close_adb_comp));
    while (!closed) acheck();
    delete gADB_async;
  } else {
    gADB_sync->finalize();
    delete gADB_sync;
  }
 
  closed = 1;
  return 0;
}

void dbfe::IMPL_close_adb_comp(tid_t tid, int err, record *res) {
  closed = 1;
}

#endif


