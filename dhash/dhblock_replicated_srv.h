#ifndef __DHBLOCK_REPLICATED_SRV__
#define __DHBLOCK_REPLICATED_SRV__

#include <dhblock_srv.h>

class location;
class chord_trigger_t;
class dhblock_replicated_srv;

struct adb_keyaux_t;
enum adb_status;

struct rjrep : public repair_job {
  rjrep (blockID key, ptr<location> s, ptr<location> w,
      ptr<dhblock_replicated_srv> bsrv, bool rev = false);
  ptr<location> src;
  const ptr<dhblock_replicated_srv> bsrv;
  const bool reversed; // should this repair _not_ be reversed again?

  void execute ();

  // Async callbacks
  void repair_retrieve_cb (dhash_stat err, 
			   ptr<dhash_block> b, 
			   route r);
  void storecb (dhash_stat err);
  void repair_send_cb (dhash_stat err, bool something, u_int32_t sz);
};

class dhblock_replicated_srv : public dhblock_srv
{
  friend struct rjrep;

  chordID last_repair;
  bool maint_pending;

  u_int64_t stale_repairs;

  // async callbacks
  void delete_cb (chordID k, str d, cb_dhstat cb, adb_status stat);
  void store_after_fetch_cb (str d, u_int32_t expiration, cb_dhstat cb, adb_status stat, 
			     adb_fetchdata_t obj);  
  void store_after_rstore_cb (chordID dbkey, cb_dhstat cb, dhash_stat astat);
  void finish_store (chordID key);

  void maintqueue (const vec<maint_repair_t> &repairs);

protected:
  qhash<chordID, vec<cbv> *, hashID> _paused_stores;

  // Mutable blocks require Merkle key tweaking
  virtual chordID idaux_to_mkey (chordID key, u_int32_t aux);
  virtual chordID id_to_dbkey (chordID key);

  virtual void real_store (chordID key, str od, str nd, u_int32_t exp, cb_dhstat cb) = 0;
  virtual void real_repair (blockID key, ptr<location> me, u_int32_t *myaux, ptr<location> them, u_int32_t *theiraux) = 0;

public:
  dhblock_replicated_srv (ptr<vnode> node, ptr<dhashcli> cli,
    dhash_ctype c, str msock, str dbsock, str dbname, ptr<chord_trigger_t> t);

  void stats (vec<dstat> &s);
  virtual void store (chordID key, str d, u_int32_t expire, cb_dhstat cb);
  virtual void fetch (chordID key, cb_fetch cb);
  virtual void generate_repair_jobs ();
};

#endif
