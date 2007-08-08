#ifndef __LIBADB_H__
#define __LIBADB_H__

#include "chord_types.h"
#include "adb_prot.h"

#include <qhash.h>

class aclnt;
class chord_trigger_t;

inline const strbuf &
strbuf_cat (const strbuf &sb, adb_status status)
{
  return rpc_print (sb, status, 0, NULL, NULL);
}

struct adb_fetchdata_t {
  chordID id;
  str data;
  u_int32_t expiration;
};

typedef callback<void, adb_status, adb_fetchdata_t>::ptr cb_fetch;
typedef callback<void, adb_status>::ptr cb_adbstat;
typedef callback<void, adb_status, u_int32_t, vec<adb_keyaux_t> >::ptr cb_getkeys;
typedef callback<void, adb_status, vec<chordID>, vec<u_int32_t> >::ptr cb_getkeyson;

typedef callback<void, adb_status, str, bool>::ptr cb_getspace_t;

class adb {
  ptr<aclnt> c;
  str dbsock_;
  str name_space;
  bool hasaux_;

  qhash<u_int32_t, chordID> getkeystab;

  bool connecting;
  void connect (ptr<chord_trigger_t> t = NULL);
  void handle_eof ();

  void initspace_cb (ptr<chord_trigger_t> t, adb_status *astat, clnt_stat stat);
  void generic_cb (adb_status *res, cb_adbstat cb, clnt_stat err);
  void fetch_cb (adb_fetchres *res, chordID key, cb_fetch cb, clnt_stat err);
  void getkeys_cb (bool getaux, adb_getkeysres *res, cb_getkeys cb, clnt_stat err);
  void getspaceinfocb (ptr<adb_getspaceinfores> res, cb_getspace_t cb, clnt_stat err);

public:
  adb (str sock_name, str name = "default", bool hasaux = false,
      ptr<chord_trigger_t> t = NULL);

  str name () const { return name_space; }
  str dbsock () const { return dbsock_; } 
  bool hasaux () const { return hasaux_; }

  void store (chordID key, str data, u_int32_t aux, u_int32_t expire, cb_adbstat cb);
  void store (chordID key, str data, cb_adbstat cb);
  void fetch (chordID key, cb_fetch cb);
  void fetch (chordID key, bool nextkey, cb_fetch cb);
  void remove (chordID key, cb_adbstat cb);
  void remove (chordID key, u_int32_t auxdata, cb_adbstat cb);
  void getkeys (u_int32_t id, cb_getkeys cb, bool ordered = false, u_int32_t batchsize = 16384, bool getaux = false);
  void sync (cb_adbstat cb);
  void expire (cb_adbstat cb, u_int32_t limit = 0, u_int32_t t = 0);

  void getspaceinfo (cb_getspace_t cb);
};

#endif
