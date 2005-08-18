#ifndef __LIBADB_H__
#define __LIBADB_H__

#include "chord_types.h"
#include "async.h"
#include "arpc.h"
#include "adb_prot.h"
#include "dbfe.h"

typedef callback<void, adb_status, chordID, str>::ptr cb_fetch;
typedef callback<void, adb_status, vec<chordID> >::ptr cb_getkeys;

chordID dbrec_to_id (ptr<dbrec> dbr);
ptr<dbrec> id_to_dbrec (chordID key, str name_space);
str dbrec_to_name (ptr<dbrec> dbr);

struct adb {

  void store (chordID key, str data, cbi cb);
  void fetch (chordID key, cb_fetch cb);
  void remove (chordID key, cbi cb);
  void getkeys (chordID start, cb_getkeys cb);
  void sync () {};
  void checkpoint () {};
  

  adb (str sock_name, str name = "default");

private:

  ptr<dbfe> db;
  ptr<aclnt> c;
  str name_space;
  
  void delete_cb (adb_status *stat, cbi cb, clnt_stat err);
  void store_cb (adb_status *res, cbi cb, clnt_stat err);
  void fetch_cb (adb_fetchres *res, chordID key, cb_fetch cb, clnt_stat err);
  void getkeys_cb (adb_getkeysres *res, cb_getkeys cb, clnt_stat err);
};

#endif
