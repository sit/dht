/*
 * dbfe.h
 *
 * Purpose: dbfe provides a front-end to BerkeleyDB..
 * 
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
 *      DBFE supports only synchronous operations.
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
 */

#ifndef _DBFE_H_
#define _DBFE_H_

#include <db.h>

int dbfe_initialize_dbenv (DB_ENV **dbep, str filename, bool join, unsigned int cachesize = 1024, str extraconf = NULL);
int dbfe_opendb (DB_ENV *dbe, DB **dbp, str filename, int flags, int mode = 0664, bool dups = false);

#ifndef DB_BUFFER_SMALL
/* DB_BUFFER_SMALL is introduced in db4.3 */
#  define DB_BUFFER_SMALL ENOMEM
#endif /* DB_BUFFER_SMALL */

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

struct dbPair {
  str key;
  str data;
  dbPair(const str &key, const str &data) : key (key), data (data) {}
};

struct optionRec {
  const char *sig;
  long value;
};


struct dbOptions {
  dbOptions();
  int addOption(const char *optionSig, long value);
  long getOption(const char *optionSig);

  vec<optionRec> options;
};

struct dbImplInfo {
  vec<const char *> supportedOptions;

  dbImplInfo() { };
};

struct dbEnumeration {
  dbEnumeration(DB *db, DB_ENV *dbe);
  ~dbEnumeration ();

  ptr<dbPair> getElement(u_int32_t flags, const str &startkey, bool getdata = false);
  ptr<dbPair> nextElement(bool getdata = false);
  ptr<dbPair> prevElement(bool getdata = false);
  ptr<dbPair> nextElement(const str &startkey, bool getdata = false);
  ptr<dbPair> lastElement(bool getdata = false);
  ptr<dbPair> firstElement(bool getdata = false);

  //ptr<dbPair> prevElement(const str &startkey); -- not implemented

  DB_TXN *txnid;
  DB_ENV *dbe;
  DBC *cursor;
};

ref<dbImplInfo> dbGetImplInfo();

class dbfe {
  DB_ENV *dbe;
  DB *db;
  
  dbfe (const dbfe &d);
 public:
  dbfe ();
  ~dbfe ();

  int opendb(const char *filename, dbOptions opts);
  int closedb();
  
  int insert (const str &key, const str &data);
  str lookup (const str &key);
  int del (const str &key);

  void checkpoint ();
  void sync ();

  ptr<dbEnumeration> enumerate ();

  // This function blocks!  It does not operate async!
  typedef callback<void, const str &, const str &>::ref traverse_act_t;
  int traverse (traverse_act_t cb);
};

#endif

