#include <dhblock_srv.h>

class dbrec;

class pmaint;
class merkle_tree;
class merkle_server;
class block_status_manager;

struct repair_state {
  ihash_entry <repair_state> link;
  bigint hashkey;
  const blockID key;
  const ptr<location> where;
  repair_state (blockID key, ptr<location> w) :
    hashkey (key.ID), key (key), where (w) {};
};

class dhblock_chash_srv : public dhblock_srv {
  // blocks that we need fetch
  enum { REPAIR_OUTSTANDING_MAX = 15 };
  ptr<adb> cache_db;

  ptr<block_status_manager> bsm;

  merkle_server *msrv;
  merkle_tree *mtree;

  pmaint *pmaint_obj;

  /* Called by merkle_syncer to notify of blocks we are succ to */
  void missing (ptr<location> from, bigint key, bool local);

  u_int32_t repair_outstanding;
  ihash<bigint, repair_state, &repair_state::hashkey, &repair_state::link, hashID> repair_q;

  timecb_t *repair_tcb;
  void repair_timer ();
  void repair_flush_q ();
  void repair (blockID k, ptr<location> to);

  void sync_cb ();

  void send_frag (blockID k, str block, ptr<location> to);
  void send_frag_cb (ptr<location> to, blockID k, dhash_stat err, bool present);
  void repair_retrieve_cb (blockID k, ptr<location> to,
			   dhash_stat err, ptr<dhash_block> b, route r);

  void repair_store_cb (chordID key, ptr<location> to, 
			int stat);
  void repair_cache_cb (blockID k, ptr<location> to, 
			adb_status stat, chordID key, str d);

public:
  dhblock_chash_srv (ptr<vnode> node, str dbname, str dbext,
      str desc);
  ~dhblock_chash_srv ();

  void start (bool randomize);
  void stop  ();

  const strbuf &key_info (const strbuf &sb);

  void store (chordID k, str d, cbi cb);

  void offer (user_args *sbp, dhash_offer_arg *arg);
  void bsmupdate (user_args *sbp, dhash_bsmupdate_arg *arg);

  void stats (vec<dstat> &s);

  merkle_server *mserv () { return msrv; };
};

