#include "dhash.h"
#include <pmaint.h>
#include <dbfe.h>
#include <dhc.h>

// Forward declarations.
class RPC_delay_args;

class dbfe;

class location;
struct hashID;

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
  bool gap ();
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

struct missing_state {
  ihash_entry <missing_state> link;
  bigint key;
  ptr<location> from;
  missing_state (bigint key, ptr<location> from) : key (key), from (from) {}
};

class dhash_impl : public dhash {
  ihash<bigint, missing_state,
    &missing_state::key, &missing_state::link, hashID> missing_q;
  enum { MISSING_OUTSTANDING_MAX = 15 };
  u_int missing_outstanding;

  u_int nreplica;
  int pk_partial_cookie;
  
  ptr<dbfe> db;
  ptr<dbfe> keyhash_db;
  ptr<vnode> host_node;
  dhashcli *cli;
  str dhcs;
  ptr<dhc> dhc_mgr;

  merkle_server *msrv;
  pmaint *pmaint_obj;
  merkle_tree *mtree;

  ptr<merkle_syncer> replica_syncer;

  ihash<chordID, store_state, &store_state::key, 
    &store_state::link, hashID> pst;
  
  ihash<int, pk_partial, &pk_partial::cookie, 
    &pk_partial::link> pk_cache;
  
  unsigned keyhash_mgr_rpcs;

  void missing (ptr<location> from, bigint key);
  void missing_retrieve_cb (bigint key, dhash_stat err, ptr<dhash_block> b,
			    route r);
  
  void sendblock (ptr<location> dst, blockID blockID,
		  callback<void, dhash_stat, bool>::ref cb);

  void keyhash_mgr_timer ();
  void keyhash_mgr_lookup (chordID key, dhash_stat err,
			   vec<chord_node> hostsl, route r);
  void keyhash_sync_done (dhash_stat stat, bool present);


  void doRPC_unbundler (ptr<location> dst, RPC_delay_args *args);


  void route_upcall (int procno, void *args, cbupcalldone_t cb);

  void doRPC (ptr<location> n, const rpc_program &prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb, 
	      cbtmo_t cb_tmo = NULL);
  void doRPC (const chord_node &n, const rpc_program &prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb,
	      cbtmo_t cb_tmo = NULL);
  void doRPC (const chord_node_wire &n, const rpc_program &prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb,
	      cbtmo_t cb_tmo = NULL);
  void doRPC_reply (svccb *sbp, void *res, 
		    const rpc_program &prog, int procno);
  void dispatch (user_args *a);
  void sync_cb ();

  void storesvc_cb (user_args *sbp, s_dhash_insertarg *arg, 
		    bool already_present, dhash_stat err);
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
  
  void store (s_dhash_insertarg *arg, bool exists, cbstore cb);
  
  void init_key_status ();

  void update_replica_list ();
  
  char responsible(const chordID& n);

  void printkeys ();
  void printkeys_walk (const chordID &k);
  void printcached_walk (const chordID &k);

  ptr<dbrec> dblookup(const blockID &i);
  int db_insert_immutable (ref<dbrec> key, ref<dbrec> data, dhash_ctype ctype);
  void db_delete_immutable (ref<dbrec> key);

  void dofetchrec (user_args *sbp, dhash_fetchrec_arg *arg);
  void dofetchrec_nexthop (user_args *sbp, dhash_fetchrec_arg *arg,
			   ptr<location> p);
  void dofetchrec_nexthop_cb (user_args *sbp, dhash_fetchrec_arg *arg,
			      ptr<dhash_fetchrec_res> res,
			      timespec t,
			      clnt_stat err);
  void dofetchrec_local (user_args *sbp, dhash_fetchrec_arg *arg);
  void dofetchrec_assembler (user_args *sbp, dhash_fetchrec_arg *arg,
			     vec<ptr<location> > succs);
  void dofetchrec_assembler_cb (user_args *sbp, dhash_fetchrec_arg *arg,
				dhash_stat s, ptr<dhash_block> b, route r);
  
  vec<ptr<location> > replicas;
  timecb_t *merkle_rep_tcb;
  timecb_t *keyhash_mgr_tcb;

  /* statistics */
  long bytes_stored;
  long keys_stored;
  long keys_replicated;
  long keys_cached;
  long keys_others;
  long bytes_served;
  long keys_served;
  long rpc_answered;

 public:
  dhash_impl (str dbname, u_int nreplica = 0);
  ~dhash_impl ();

  void replica_maintenance_timer (u_int index);

  void init_after_chord (ptr<vnode> node);

  void print_stats ();
  void stop ();
  void fetch (blockID id, int cookie, cbvalue cb);

  dhash_stat key_status (const blockID &n);
};
