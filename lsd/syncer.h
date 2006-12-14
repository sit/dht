#include "dhash_types.h"
#include "libadb.h"

class locationtable;
class location;
class block_status_manager;
class merkle_tree;
class merkle_syncer;
struct RPC_delay_args;

typedef callback<void, ptr<location> >::ref  cb_location;
typedef callback<void, vec<ptr<location> > >::ref cb_locationlist;

class syncer {
  ptr<locationtable> locations;

  dhash_ctype ctype;

  u_int dfrags;
  u_int efrags;

  ptr<merkle_tree> tmptree;
  ptr<merkle_syncer> replica_syncer;
  ptr<location> host_loc;
  ptr<adb> db;
  ptr<aclnt> curr_client;
  ptr<location> curr_dst;
  str db_prefix;

  u_int cur_succ;

  int replica_timer;

public:

  syncer::syncer (ptr<locationtable> locations,
		  ptr<location> h,
		  str dbdir,
		  str dbname,
		  str dbext,
		  dhash_ctype ctype,
		  u_int dfrags = 0,
		  u_int efrags = 0);
  ~syncer ();
  
protected:
  void doRPC (const rpc_program &prog,
	      int procno, const void *in, void *out, aclnt_cb cb);

  void update_pred (cb_location cb);
  void update_pred_cb (cb_location cb,  chord_noderes *res, clnt_stat err);
  
  void get_succlist (cb_locationlist cb);
  void get_succlist_cb (chord_nodelistres *res,
			cb_locationlist cb,
			clnt_stat status);
  
  
  void sync_replicas ();
  void sync_replicas_predupdated (ptr<location> pred);
  void sync_replicas_gotsucclist (ptr<location> pred,
				  vec<ptr<location> > succs);
  void tcp_connected (ptr<location> pred,
		      vec<ptr<location> > succs, int fd);

  void doRPC_unbundler (RPC_delay_args *args);
  void missing (ptr<location> from, ptr<merkle_tree> tmptree,
		bigint key, bool missing_local);
};
