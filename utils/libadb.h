#ifndef __LIBADB_H__
#define __LIBADB_H__

#include "chord_types.h"
#include "adb_prot.h"

#include <qhash.h>

class aclnt;
class location;
struct block_info;

typedef callback<void, adb_status, chordID, str>::ptr cb_fetch;
typedef callback<void, adb_status>::ptr cb_adbstat;
typedef callback<void, adb_status, u_int32_t, vec<adb_keyaux_t> >::ptr cb_getkeys;
typedef callback<void, adb_status, vec<chordID>, vec<u_int32_t> >::ptr cb_getkeyson;

typedef callback<void, clnt_stat, adb_status, vec<block_info> >::ref cbvblock_info_t;
typedef callback<void, clnt_stat, adb_status, block_info>::ref cbblock_info_t;

struct block_info {
  chordID k;
  vec<chord_node> on;
  vec<u_int32_t> aux;
  block_info () {};
  block_info (chordID k) : k (k) {};
  block_info (const block_info &b) : k (b.k), on (b.on), aux (b.aux) {};

  block_info& operator= (const block_info &b) {
    if( this != &b ) {
      k = b.k;
      on = b.on;
      aux = b.aux;
    }
    return *this;
  }
};

class adb {
  ptr<aclnt> c;
  str name_space;
  bool hasaux;

  qhash<u_int32_t, chordID> getkeystab;

  vec<adb_updatearg *> batched_updates;
  timecb_t *next_batch;
  enum {
    UPDATE_BATCH_SECS = 1,
    UPDATE_BATCH_MAX_SIZE = 128
  };

  void initspace_cb (adb_status *astat, clnt_stat stat);
  void generic_cb (adb_status *res, cb_adbstat cb, clnt_stat err);
  void fetch_cb (adb_fetchres *res, chordID key, cb_fetch cb, clnt_stat err);
  void getkeys_cb (bool getaux, adb_getkeysres *res, cb_getkeys cb, clnt_stat err);
  void getkeyson_cb (bool getaux, adb_getkeysres *res, cb_getkeyson cb, clnt_stat err);
  void getblockrangecb (ptr<adb_getblockrangeres> res, cbvblock_info_t cb, clnt_stat err);
  void getinfocb (chordID block, ptr<adb_getinfores> res, cbblock_info_t cb, clnt_stat err);
  void batch_update ();

public:
  adb (str sock_name, str name = "default", bool hasaux = false);

  void store (chordID key, str data, u_int32_t auxdata, cb_adbstat cb);
  void store (chordID key, str data, cb_adbstat cb);
  void fetch (chordID key, cb_fetch cb);
  void fetch (chordID key, bool nextkey, cb_fetch cb);
  void remove (chordID key, cb_adbstat cb);
  void getkeys (u_int32_t id, cb_getkeys cb, bool ordered = false, u_int32_t batchsize = 16384, bool getaux = false);
  void sync ();

  void getblockrange (const chordID &start, const chordID &stop,
      int extant, int count, cbvblock_info_t cb);
  void getkeyson (const ptr<location> n, const chordID &start,
      const chordID &stop, cb_getkeyson cb);
  void update (const chordID &block, const ptr<location> n, bool present, 
	       bool batchable = false);
  void update (const chordID &block, const ptr<location> n, u_int32_t auxdata, 
	       bool present, bool batchable = false);
  void getinfo (const chordID &block, cbblock_info_t cb);
};

#endif
