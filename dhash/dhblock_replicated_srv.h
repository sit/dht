#ifndef __DHBLOCK_REPLICATED_SRV__
#define __DHBLOCK_REPLICATED_SRV__

#include <dhblock_srv.h>

class location;
class dhblock_replicated_srv;

struct adb_keyaux_t;
enum adb_status;

struct rjrep : public repair_job {
  rjrep (blockID key, ptr<location> w, ptr<dhblock_replicated_srv> bsrv);
  const ptr<dhblock_replicated_srv> bsrv;

  void execute ();

  // Async callbacks
  void repair_retrieve_cb (dhash_stat err, 
			   ptr<dhash_block> b, 
			   route r);
  void repair_send_cb (dhash_stat err, bool something);
  bool repair (blockID k, ptr<location> to);
};

class dhblock_replicated_srv : public dhblock_srv
{
  friend struct rjrep;

  // async callbacks
  void delete_cb (chordID k, str d, cb_dhstat cb, adb_status stat);
  void store_after_fetch_cb (str d, cb_dhstat cb, adb_status stat, 
			     chordID key, str old_data);  
  void store_after_rstore_cb (chordID dbkey, cb_dhstat cb, dhash_stat astat);
  void finish_store (chordID key);

  void localqueue (clnt_stat err, adb_status stat, vec<block_info> blocks);

protected:
  const dhash_ctype ctype;

  qhash<chordID, vec<cbv> *, hashID> _paused_stores;


  // Mutable blocks require Merkle key tweaking
  virtual chordID idaux_to_mkey (chordID key, u_int32_t aux);
  virtual chordID id_to_dbkey (chordID key);

  virtual void real_store (chordID key, str od, str nd, cb_dhstat cb) = 0;
  virtual void real_repair (blockID key, ptr<location> me, u_int32_t *myaux, ptr<location> them, u_int32_t *theiraux) = 0;

public:
  dhblock_replicated_srv (ptr<vnode> node, ptr<dhashcli> cli,
			  str dbname, str dbext, str desc,
                          dhash_ctype ctype, cbv donecb);

  virtual void store (chordID key, str d, cb_dhstat cb);
  virtual void fetch (chordID key, cb_fetch cb);
  virtual void generate_repair_jobs ();
};

#endif
