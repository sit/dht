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

#include <async.h>
#include "dbfe.h"

#ifdef DMALLOC
#include "dmalloc.h"
#endif

#define CACHE_OPT "opt_cachesize"
#define CREATE_OPT "opt_create"
#define DBENV_OPT    "opt_dbenv"
#define FLAG_OPT   "opt_flag"
#define JOIN_OPT  "opt_join"
#define PERM_OPT "opt_permissions"

// Choose the best stablest txn protected level available
static const int isolated_read_flag = 
#if (DB_VERSION_MAJOR < 4) || \
    ((DB_VERSION_MAJOR == 4) && (DB_VERSION_MINOR < 3))
    DB_DIRTY_READ
#elif ((DB_VERSION_MAJOR == 4) && (DB_VERSION_MINOR == 3))
    DB_DEGREE_2
#else 
    DB_READ_COMMITTED
#endif 
    ;

///////////////// static /////////////////////

ref<dbImplInfo>
dbGetImplInfo() {
  ref<dbImplInfo> info = New refcounted<dbImplInfo>();
  info->supportedOptions.push_back(CACHE_OPT);
  info->supportedOptions.push_back(CREATE_OPT);
  info->supportedOptions.push_back(DBENV_OPT);
  info->supportedOptions.push_back(FLAG_OPT);
  info->supportedOptions.push_back(JOIN_OPT);
  info->supportedOptions.push_back(PERM_OPT);
  return info;
}

//////////////////// dbOptions ///////////////////////
dbOptions::dbOptions()  {}

int verifyOption (const char *optionSig) {
  vec<const char *> allowed = dbGetImplInfo()->supportedOptions;
  for (unsigned int i=0; i < allowed.size(); i++) 
    if (memcmp(allowed[i], optionSig, strlen(allowed[i])) == 0) return 1;
  return 0;
}

int
dbOptions::addOption(const char *optionSig, long value) {
  if (!verifyOption(optionSig)) return EINVAL;

  optionRec optr;
  optr.sig = optionSig;
  optr.value = value;
  options.push_back(optr);
  return 0;
}

long
dbOptions::getOption(const char *optionSig) {
  for (unsigned int i=0; i < options.size(); i++) 
    if (memcmp(options[i].sig, optionSig, strlen(options[i].sig)) == 0) return options[i].value;
  return -1;
}


//////////////////////dbEnumeration//////////////////////

dbEnumeration::dbEnumeration (DB *db, DB_ENV *dbe) :
  txnid (NULL),
  dbe (dbe),
  cursor (NULL)
{
  int r = 0;
  if (dbe) {
    r = dbfe_txn_begin (dbe, &txnid);
    if (r) {
      const char *path (NULL);
      dbe->get_home (dbe, &path);
      fatal << "enumeration error for " << path << ": "
	    << db_strerror (r) << "\n";
    }
  }

  r = db->cursor(db, txnid, &cursor, isolated_read_flag);
  if (r) {
    const char *path (NULL);
    dbe->get_home (dbe, &path);
    fatal << "enumeration error for " << path << ": "
          << db_strerror (r) << "\n";
  }
}

dbEnumeration::~dbEnumeration() {
  cursor->c_close (cursor);
  if (txnid) {
    dbfe_txn_commit (dbe, txnid);
    txnid = NULL;
  }
}

ptr<dbPair>
dbEnumeration::getElement(u_int32_t flags, const str &startkey,
    bool getdata)
{
  DBT key;
  bzero(&key, sizeof(key));
  if (startkey) {
    key.size = startkey.len ();
    key.data = (void *) startkey.cstr ();
  }

  DBT data;
  bzero(&data, sizeof(data));
  if (!getdata)
    data.flags = DB_DBT_PARTIAL;

  int err = cursor->c_get(cursor, &key, &data, flags);
  if (err) {
    //    warn << "db error: " << db_strerror(err) << "\n";
    return NULL;
  }
  str keyrec (static_cast<char *> (key.data), key.size);
  str valrec = NULL;
  if (getdata) 
    valrec = str (static_cast<char *> (data.data), data.size);
  return New refcounted<dbPair>(keyrec, valrec);
}

ptr<dbPair>
dbEnumeration::nextElement(bool getdata)
{
  return getElement (DB_NEXT, NULL, getdata);
}

ptr<dbPair>
dbEnumeration::prevElement(bool getdata)
{
  return getElement (DB_PREV, NULL, getdata);
}

ptr<dbPair>
dbEnumeration::nextElement(const str &startkey, bool getdata)
{
  return getElement(DB_SET_RANGE, startkey, getdata);
}

ptr<dbPair>
dbEnumeration::lastElement(bool getdata)
{
  return getElement(DB_LAST, NULL, getdata);
}

ptr<dbPair>
dbEnumeration::firstElement(bool getdata)
{
  return getElement(DB_FIRST, NULL, getdata);
}

////////////////////// dbfe //////////////////////////////
dbfe::dbfe () : dbe (NULL), db (NULL)
{
}

dbfe::~dbfe() {
  closedb ();
}

int dbfe::opendb (const char *filename, dbOptions opts) { 
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

#if (DB_VERSION_MAJOR < 4) || \
    ((DB_VERSION_MAJOR == 4) && (DB_VERSION_MINOR < 3))
  // BerkeleyDB 4.3 introduces support for level 2 isolation.
  // Until then, we need support for dirtier stuff, if requested.
  flags |= DB_DIRTY_READ;
#endif 

  if (do_dbenv) {
    r = dbfe_initialize_dbenv (&dbe, filename, join >= 0, cachesize);
    if (r){
      warn << "dbe->open returned " << r << " which is " << db_strerror(r) << "\n";
      return r;
    }
    r = dbfe_opendb (dbe, &db, "db", flags, mode);
  } else {
    r = dbfe_opendb (dbe, &db, filename, flags, mode);
  }
  return r;
}


void
dbfe::checkpoint ()
{
  if (dbe)
#if (DB_VERSION_MAJOR < 4)
     txn_checkpoint (dbe, 0, 0, 0);
#else
     dbe->txn_checkpoint (dbe, 0, 0, 0);
#endif
}

int dbfe::insert (const str &key, const str &data) { 
  DB_TXN *t = NULL;
  int r = 0, tr = 0;
  DBT skey, content;

  bzero(&skey, sizeof(skey));
  bzero(&content, sizeof(content));
  content.size = data.len ();
  content.data = (void *) (data.cstr ());
  skey.size = key.len ();
  skey.data = (void *) (key.cstr ());

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

str 
dbfe::lookup (const str &key) 
{ 
  DBT skey, content;
  bzero(&skey, sizeof(skey));
  skey.size = key.len ();
  skey.data = (void *) (key.cstr ());
  bzero(&content, sizeof(content));
  int r=0;

  
  if ((r = db->get(db, NULL, &skey, &content, 0)) != 0) return NULL;
  str ret (static_cast<char *> (content.data), content.size);

  // warnx << "return " << content.size << "\n";

  return ret;
} 

int 
dbfe::del (const str &key) {
  DB_TXN *t = NULL;
  int err, terr;
  DBT dkey;
  bzero(&dkey, sizeof(dkey));
  dkey.size = key.len ();
  dkey.data = (void *) (key.cstr ());

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

int 
dbfe::closedb () { 
  int r;
  r = db->close(db, 0);
  if(dbe) dbe->close(dbe, 0);
  return r;
}

ptr<dbEnumeration> dbfe::enumerate ()
{
  return New refcounted<dbEnumeration>(db, dbe);
}

void
dbfe::sync () 
{
  if (dbe)
    return;
  db->sync (db, 0L);
}

int
dbfe::traverse (traverse_act_t cb)
{
  /* Adapted from BerkeleyDB get_bulk example code */
  DBC *dbcp;
  DBT key, data, onedata;
  DB_TXN *txnid = NULL;
  size_t retklen, retdlen;
  void *retkey, *retdata;
  int ret, t_ret;
  void *p;

  bzero (&key, sizeof (key));
  bzero (&data, sizeof (data));
  bzero (&onedata, sizeof (onedata));

  /* Review the database in 5MB chunks. */
#define BUFFER_LENGTH (5 * 1024 * 1024)
  if ((data.data = malloc(BUFFER_LENGTH)) == NULL) {
    return (errno);
  }
  data.ulen = BUFFER_LENGTH;
  data.flags = DB_DBT_USERMEM;

  ret = dbfe_txn_begin (dbe, &txnid);
  if (ret) {
    db->err (db, ret, "DBE->txn_begin");
    free (data.data);
    return (ret);
  }

  /* Acquire a cursor for the database. */
  if ((ret = db->cursor(db, txnid, &dbcp, isolated_read_flag)) != 0) {
    db->err (db, ret, "DB->cursor");
    dbfe_txn_abort (dbe, txnid);
    free (data.data);
    return (ret);
  }

  for (;;) {
    /*
     * Acquire the next set of key/data pairs.
     */
    if ((ret = dbcp->c_get (dbcp,
	    &key, &data, DB_MULTIPLE_KEY | DB_NEXT)) != 0)
    {
      if (ret == DB_BUFFER_SMALL) {
	/* Single key too big! Allow BDB to allocate memory. */
	ret = dbcp->c_get (dbcp, &key, &onedata, DB_NEXT);
	if (ret != 0) {
	  if (ret != DB_NOTFOUND)
	    db->err (db, ret, "DBcursor->c_get one");
	  break;
	}
	str rk ((const char *) key.data, key.size);
	str rd ((const char *) onedata.data, onedata.size);
	(*cb) (rk, rd);
	continue;
      } else
      if (ret != DB_NOTFOUND)
	db->err (db, ret, "DBcursor->c_get bulk");

      break;
    }

    for (DB_MULTIPLE_INIT (p, &data);;) {
      DB_MULTIPLE_KEY_NEXT (p, &data, retkey, retklen, retdata, retdlen);
      if (p == NULL)
	break;
      str rk ((char *) retkey, retklen);
      str rd ((char *) retdata, retdlen);
      (*cb) (rk, rd);
    }
  }

  if ((t_ret = dbcp->c_close (dbcp)) != 0) {
    db->err(db, ret, "DBcursor->close");
    if (ret == 0)
      ret = t_ret;
  }

  free (data.data);
  if (ret != 0)
    dbfe_txn_abort (dbe, txnid);
  else
    dbfe_txn_commit (dbe, txnid);
  return (ret);
}
