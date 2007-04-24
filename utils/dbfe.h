/*
 * dbfe.h
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
 * Purpose: dbfe provides a common interface to the DB3 (aka
 *          sleepycat) and ADB databases.
 * 
 * Use: 
 * 
 *   Compiling: 
 *   
 *   The database decision must be made at compile time. To
 *   use the sleepycat database define the SLEEPYCAT symbol otherwise
 *   ADB will be used. The simplest way to choose the database is to
 *   issue the --with-db3 option to the configure script.
 *
 *   Linking: 
 *
 *   Link any applicatio using this front end against
 *   libsfsrodb. If SLEEPYCAT is defined, you must link against -db,
 *   otherwise link against ../adb/libadb.a. (linking against $(DBLIB)
 *   will usually suffice to fix this dependency).
 *
 *  Use: 
 *
 *    1) Create a new dbfe object: dbfe *db = new dbfe();
 *    
 *    2) open a database using the db->open call
 *    
 *    2.1) specifying options The open call takes a dbOptions
 *       structure which specifies options to be used when creating
 *       the database. If opts is a dbOptions struct, use
 *       opts.addOptions() to specify options.
 *
 *        for example: opts.addOption("opt_flags", DB_CREATE)
 *        specifies flags to be passed to db->open in DB3. To see
 *        which flags are available call dbGetImplInfo and examine
 *        the supportedOptions field
 *
 *   3) Database operations
 *   
 *   3.1) async v. sync operation
 *      DBFE supports both async and sync operation using function overloading. To make
 *      an async call, use the version of the function that includes a callback 
 *
 *     i.e. void insert(str key, str data, callback<void, int>::ref cb)
 *            v.
 *       int insert(str key, str data)
 *
 *     Note that when using ADB you must take some precautions using async mode:
 *       -if you specify the async option you may not use any sync calls
 *      -if your program calls amain() you _must_ use the async option
 *
 *  3.2) Iteration
 *  
 *     example:
 *     ptr<dbEnumeration> it = db->enumerate();
 *     while (it->hasMoreElements())
 *        ptr<dbPair> = it->nextElement();
 *
 *     warning: the hasMoreElements call is not implemented for
 *     sleepycat. To work around this, call nextElement until it
 *     returns null.
 *    
 *
 *
 */



#ifndef _DBFE_H_
#define _DBFE_H_

#include "vec.h"
#include "async.h"
#include "callback.h"

#ifdef SLEEPYCAT
# ifndef HAVE_DB3_H
#  include <db.h>
# else /* HAVE_DB3_H */
#  include <db3.h>
# endif /* HAVE_DB3_H */
#else /* !SLEEPYCAT */
#include "btree.h"
#include "btreeSync.h"
#include "btreeDispatch.h"
#endif /* !SLEEPYCAT */

#ifdef SLEEPYCAT
int dbfe_initialize_dbenv (DB_ENV **dbep, str filename, bool join, unsigned int cachesize = 1024);
int dbfe_opendb (DB_ENV *dbe, DB **dbp, str filename, int flags, int mode = 0664, bool dups = false);

inline int
dbfe_txn_begin (DB_ENV *dbe, DB_TXN **t)
{
  int r;
#if DB_VERSION_MAJOR >= 4
  r = dbe->txn_begin (dbe, NULL, t, 0);
#else
  r = txn_begin (dbe, NULL, t, 0);
#endif
  return r;
}

inline int
dbfe_txn_abort (DB_ENV *dbe, DB_TXN *t)
{
  int r;
#if DB_VERSION_MAJOR >= 4
  r = t->abort (t);
#else
  r = txn_abort (t, 0);
#endif 
  return r;
}

inline int
dbfe_txn_commit (DB_ENV *dbe, DB_TXN *t)
{
  int r;
#if DB_VERSION_MAJOR >= 4
  r = t->commit (t, 0);
#else
  r = txn_commit (t, 0);
#endif 
  return r;
}
#endif /* SLEEPYCAT */

struct dbPair {
  str key;
  str data;
  dbPair(const str &key, const str &data) : key (key), data (data) {}
};

struct optionRec {
  char *sig;
  long value;
};


struct dbOptions {
  
  dbOptions();
  int addOption(char *optionSig, long value);
  long getOption(char *optionSig);

  vec<optionRec> options;
  int sel;
};

struct dbImplInfo {
  vec<char *> supportedOptions;

  dbImplInfo() { };
};

struct dbEnumeration {
#ifdef SLEEPYCAT
  dbEnumeration(DB *db, DB_ENV *dbe);
  ~dbEnumeration ();
#else
  ~dbEnumeration ();
  dbEnumeration(btreeSync *adb);
  dbEnumeration(btreeDispatch *adb);
  void ne_cb(callback<void, ptr<dbPair> >::ref cb, tid_t, int, record *);
#endif

  ptr<dbPair> getElement(u_int32_t flags, const str &startkey);
  ptr<dbPair> nextElement();
  ptr<dbPair> prevElement();
  ptr<dbPair> nextElement(const str &startkey);
  ptr<dbPair> lastElement();
  ptr<dbPair> firstElement();

  //ptr<dbPair> prevElement(const str &startkey); -- not implemented
  char hasMoreElements();  // broken -- don't use

#ifdef SLEEPYCAT
  DB* db_sync;
  DBC *cursor;
  char cursor_init;
#else
  bIteration *it;
  btreeSync *ADB_sync;
  btreeDispatch *ADB_async;
#endif
  char async;
};

ref<dbImplInfo> dbGetImplInfo();

class dbfe {
  typedef callback<void, int>::ptr errReturn_cb;
  typedef callback<void, const str &>::ptr itemReturn_cb;
  typedef callback<int, char *, dbOptions>::ptr open_cb;
  typedef callback<int>::ptr ivcb;
  
  typedef callback<int, const str &, const str &>::ptr insert_cb;
  typedef callback<str, const str &>::ptr lookup_cb;
  
  typedef callback<int, const str &>::ptr  delete_cb;
  typedef callback<void, const str &, errReturn_cb>::ptr delete_cb_async;

  typedef callback<void, const str &, const str &, errReturn_cb >::ptr insert_cb_async;
  typedef callback<void, const str &, itemReturn_cb >::ptr lookup_cb_async;

  static void itemReturn_dummy_cb (itemReturn_cb cb, str ret);
  static void errReturn_dummy_cb (errReturn_cb cb, int err);


  open_cb create_impl;
  open_cb  open_impl;
  ivcb close_impl;

  insert_cb insert_impl;
  callback<void>::ptr checkpoint_impl;
  lookup_cb lookup_impl;

  delete_cb delete_impl;
  delete_cb_async delete_impl_async;

  insert_cb_async  insert_impl_async;
  lookup_cb_async lookup_impl_async;
  callback<ptr<dbEnumeration> >::ptr make_enumeration;

#ifdef SLEEPYCAT
  DB_ENV* dbe;
  DB* db;
  int IMPL_open_sleepycat(char *filename, dbOptions opts);
  int IMPL_close_sleepycat();
  int IMPL_create_sleepycat(char *filename, dbOptions opts);
  int IMPL_insert_sync_sleepycat(const str &key, const str &data);
  str IMPL_lookup_sync_sleepycat(const str &key);
  void IMPL_insert_async_sleepycat(const str &key, const str &data, errReturn_cb cb);
  void IMPL_checkpoint_sleepycat ();
  void IMPL_lookup_async_sleepycat(const str &key, itemReturn_cb cb);
  ptr<dbEnumeration> IMPL_make_enumeration_sleepycat();
  void IMPL_delete_async_sleepycat(const str &key, errReturn_cb cb);
  int IMPL_delete_sync_sleepycat(const str &key);
  void IMPL_sync ();

#else
  #error ADB is marked broken

  btreeSync *gADB_sync;
  btreeDispatch *gADB_async;

  ptr<dbEnumeration> IMPL_make_enumeration_adb();
  int IMPL_open_adb(char *filename, dbOptions opts);
  int IMPL_close_adb();
  void IMPL_close_adb_comp(tid_t tid, int err, record *res);
  int IMPL_create_adb(char *filename, dbOptions opts);
  int IMPL_insert_sync_adb(const str &key, const str &data);
  str IMPL_lookup_sync_adb(const str &key);
  void IMPL_insert_async_adb(const str &key, const str &data, errReturn_cb cb);
  void IMPL_insert_async_adb_comp(errReturn_cb cb, tid_t tid, int err, record *res);
  void IMPL_lookup_async_adb(const str &key, itemReturn_cb cb);
  void IMPL_lookup_async_adb_comp(itemReturn_cb cb, tid_t tid, int err, record *res);
#endif
  
  char closed;
  
 public:

  dbfe();
  ~dbfe();

  int createdb(char *filename, dbOptions opts) { return (*create_impl)(filename, opts); };
  int opendb(char *filename, dbOptions opts)  { return (*open_impl)(filename, opts); };
  int closedb()  { return (*close_impl)(); };
  
  int insert(const str &key, const str &data)  { return (*insert_impl)(key, data); };
  str lookup(const str &key) { return (*lookup_impl)(key); };
  int del(const str &key) { return (*delete_impl) (key); };

  void insert(const str &key, const str &data, callback<void, int>::ref cb)  
    { return (*insert_impl_async)(key, data, cb); };
  void lookup(const str &key, callback<void, const str & >::ref cb)
    { return (*lookup_impl_async)(key, cb); };
  void del(const str &key, errReturn_cb cb) 
    { return (*delete_impl_async) (key, cb); };
  void sync () 
    { IMPL_sync (); };

  void checkpoint ()
  {  (*checkpoint_impl) (); };



  ptr<dbEnumeration> enumerate() { return (*make_enumeration)(); };
};

#endif

