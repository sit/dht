#include <coord.h>
#include <id_utils.h>
#include <locationtable.h>
#include <block_status.h>
#include <merkle_tree.h>
#include <merkle_syncer.h>
#include <dbfe.h>

typedef callback<void, ptr<location> >::ref  cb_location;
typedef callback<void, vec<ptr<location> > >::ref cb_locationlist;

class syncer {
  ref<block_status_manager> bsm;
  ptr<locationtable> locations;

  merkle_tree *tmptree;
  ptr<merkle_syncer> replica_syncer;
  ptr<location> host_loc;
  ptr<dbfe> db;

  u_int cur_succ;
  u_int efrags, dfrags;

  int replica_timer;

public:

  syncer::syncer (ptr<locationtable> locations,
		  ptr<location> h,
		  str dbname,
		  int efrags, int dfrags);
  
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

  void doRPC_unbundler (ptr<location> dst, RPC_delay_args *args);
  void missing (ptr<location> from,
		vec<ptr<location> > succs,
		bigint key, bool missingLocal);

  void check_replication (chordID key, vec<ptr<location> > succs);
  
};
