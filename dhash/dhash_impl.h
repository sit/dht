#include "dhash.h"
#include <dbfe.h>

// Forward declarations.
class RPC_delay_args;

class dbfe;

class merkle_server;
class merkle_syncer;
class merkle_tree;

class dhashcli;

// Helper structs/classes
struct store_chunk {
  store_chunk *next;
  unsigned int start;
  unsigned int end;

  store_chunk (unsigned int s, unsigned int e, store_chunk *n) : next(n), start(s), end(e) {};
  ~store_chunk () {};
};

struct store_state {
  chordID key;
  unsigned int size;
  store_chunk *have;
  char *buf;
  route path;

  ihash_entry <store_state> link;
   
  store_state (chordID k, unsigned int z) : key(k), 
    size(z), have(0), buf(New char[z]) { };

  ~store_state ()
  { 
    delete[] buf; 
    store_chunk *cnext;
    for (store_chunk *c=have; c; c=cnext) {
      cnext = c->next;
      delete c;
    }
  };
  bool addchunk (unsigned int start, unsigned int end, void *base);
  bool iscomplete ();
};

struct pk_partial {
  ptr<dbrec> val;
  int bytes_read;
  int cookie;
  ihash_entry <pk_partial> link;

  pk_partial (ptr<dbrec> v, int c) : val (v), 
		bytes_read (0),
		cookie (c) {};
};

struct keyhash_meta {
  long version;
};

class dhash_impl : public dhash {
  u_int nreplica;
  int kc_delay;
  int rc_delay;
  int ss_mode;
  int pk_partial_cookie;
  
  ptr<dbfe> db;
  ptr<dbfe> keyhash_db;
  ptr<vnode> host_node;
  dhashcli *cli;
  ptr<route_factory> r_factory;

  merkle_server *msrv;
  merkle_tree *mtree;
  qhash<chordID, ptr<merkle_syncer>, hashID> active_syncers;

  chordID replica_syncer_dstID;
  ptr<merkle_syncer> replica_syncer;

  chordID partition_left;
  chordID partition_right;
  ptr<merkle_syncer> partition_syncer;
  
  ptr<dbEnumeration> partition_enumeration;
  chordID partition_current;

  ihash<chordID, store_state, &store_state::key, 
    &store_state::link, hashID> pst;

  ihash<int, pk_partial, &pk_partial::cookie, 
    &pk_partial::link> pk_cache;
  
  qhash<int, cbblockuc_t> bcpt;
  qhash<int, cbstorecbuc_t> scpt;

  unsigned keyhash_mgr_rpcs;

  void missing (chord_node from, bigint key);
  void missing_retrieve_cb (bigint key, dhash_stat err, ptr<dhash_block> b, route r);
  
  void sendblock (chord_node dst, bigint blockID, bool last, callback<void>::ref cb);
  void sendblock_cb (callback<void>::ref cb, dhash_stat err, chordID blockID);

  void keyhash_mgr_timer ();
  void keyhash_mgr_lookup (chordID key, dhash_stat err, chordID host, route r);
  void keyhash_sync_done ();
  void partition_maintenance_lookup_cb (dhash_stat err, chordID hostID, route r);
  void partition_maintenance_pred_cb (chordID predID, net_address addr, chordstat stat);

  void partition_maintenance_lookup_cb2 (bigint key, dhash_stat err, chordID hostID, route r);
  void partition_maintenance_succs_cb2 (bigint key, vec<chord_node> succs, chordstat err);
  void partition_maintenance_store2 (bigint key, vec<chord_node> succs, u_int already_count);
  void partition_maintenance_store_cb2 (bigint key, vec<chord_node> succs,
					u_int already_count, ref<dhash_storeres> res,
					clnt_stat err);
  
  void doRPC_unbundler (chord_node dst, RPC_delay_args *args);


  void route_upcall (int procno, void *args, cbupcalldone_t cb);

  void doRPC (chordID ID, const rpc_program &prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb);
  void doRPC_reply (svccb *sbp, void *res, 
		    const rpc_program &prog, int procno);
  void dispatch (user_args *a);
  void sync_cb ();

  void storesvc_cb (user_args *sbp, s_dhash_insertarg *arg, bool already_present, dhash_stat err);
  void fetch_cb (int cookie, cbvalue cb,  ptr<dbrec> ret);
  dhash_fetchiter_res * block_to_res (dhash_stat err, s_dhash_fetch_arg *arg,
				      int cookie, ptr<dbrec> val);
  void fetchiter_gotdata_cb (cbupcalldone_t cb, s_dhash_fetch_arg *farg,
			     int cookie, ptr<dbrec> val, dhash_stat stat);
  void fetchiter_sbp_gotdata_cb (user_args *sbp, s_dhash_fetch_arg *farg,
				 int cookie, ptr<dbrec> val, dhash_stat stat);
  void sent_block_cb (dhash_stat *s, clnt_stat err);

  void append (ref<dbrec> key, ptr<dbrec> data,
	       s_dhash_insertarg *arg,
	       cbstore cb);
  void append_after_db_store (cbstore cb, chordID k, int stat);
  void append_after_db_fetch (ref<dbrec> key, ptr<dbrec> new_data,
			      s_dhash_insertarg *arg, cbstore cb,
			      int cookie, ptr<dbrec> data, dhash_stat err);
  
  void store (s_dhash_insertarg *arg, cbstore cb);
  
  void init_key_status ();

  void update_replica_list ();
  
  char responsible(const chordID& n);

  void printkeys ();
  void printkeys_walk (const chordID &k);
  void printcached_walk (const chordID &k);

  void block_cached_loc (ptr<s_dhash_block_arg> arg, 
			 chordID ID, bool ok, chordstat stat);

  void dbwrite (ref<dbrec> key, ref<dbrec> data);
  void dbdelete (ref<dbrec> key);



  chordID pred;
  vec<chord_node> replicas;
  timecb_t *check_replica_tcb;
  timecb_t *merkle_rep_tcb;
  timecb_t *merkle_part_tcb;
  timecb_t *keyhash_mgr_tcb;

  /* statistics */
  long bytes_stored;
  long keys_stored;
  long keys_replicated;
  long keys_cached;
  long bytes_served;
  long keys_served;
  long rpc_answered;

 public:
  dhash_impl (str dbname, u_int nreplica = 0, int ss_mode = 0);
  ~dhash_impl ();
  
  void replica_maintenance_timer (u_int index);
  void partition_maintenance_timer ();

  void init_after_chord (ptr<vnode> node, ptr<route_factory> r_fact);

  void print_stats ();
  void stop ();
  void fetch (chordID id, int cookie, cbvalue cb);
  void register_block_cb (int nonce, cbblockuc_t cb);
  void unregister_block_cb (int nonce);
  void register_storecb_cb (int nonce, cbstorecbuc_t cb);
  void unregister_storecb_cb (int nonce);

  dhash_stat key_status (const chordID &n);
};
