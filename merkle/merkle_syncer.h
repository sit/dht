#ifndef _MERKLE_SYNCER_H_
#define _MERKLE_SYNCER_H_

#include "merkle_hash.h"
#include "merkle_node.h"
#include "merkle_tree.h"
#include "merkle_sync_prot.h"
#include <bigint.h>
#include <chord.h>

// see comment in merkle_syncer::doRPC for this work around.
#include "location.h"
typedef callback<void, RPC_delay_args *>::ref rpcfnc_t;
//typedef callback<void, rpc_program, int, ptr<void>, void *, aclnt_cb>::ref rpcfnc_t;


typedef callback<void, bigint, bool, callback<void>::ref>::ref sndblkfnc_t;

class merkle_syncer {
 private:
  void setdone ();
  void error (str err);
  void timeout ();

  enum { IDLETIMEOUT = 30 };
  bool idle;
  ptr<bool> deleted;

 public:
  typedef enum {
    BIDIRECTIONAL,
    UNIDIRECTIONAL
  } mode_t;

  mode_t mode;


  merkle_tree *ltree; // local tree
  rpcfnc_t rpcfnc;
  sndblkfnc_t sndblkfnc;


  str fatal_err;
  bool sync_done;

  bigint rngmin;
  bigint rngmax;

  timecb_t *tcb;
  int pending_rpcs;
  int receiving_blocks;
  uint64 num_sends_pending;
  db_iterator *sendblocks_iter;

  vec<pair<merkle_rpc_node, int> > st;

  void dump ();
  merkle_syncer (merkle_tree *ltree, rpcfnc_t rpcfnc, sndblkfnc_t sndblkfnc);
  ~merkle_syncer ();
  void doRPC (int procno, ptr<void> in, void *out, aclnt_cb cb);
  void send_some ();
  void next (void);
  void sendblock (merkle_hash key, bool last);
  void sendblock_cb ();
  void getblocklist (vec<merkle_hash> keys);
  void getblocklist_cb (ref<getblocklist_res> res, ptr<bool> del, 
			clnt_stat err);

  bool done () { return sync_done; }
  void sync (bigint _rngmin, bigint _rngmax, mode_t m);
  void getnode (u_int depth, const merkle_hash &prefix);
  void getnode_cb (ref<getnode_arg> arg, ref<getnode_res> res, 
		   ptr<bool> del, clnt_stat err);
  void getblockrange (merkle_rpc_node *rnode);
  void getblockrange_cb (ref<getblockrange_arg> arg, 
			 ref<getblockrange_res> res, 
			 ptr<bool> del,
			 clnt_stat err);

  bool inrange (const merkle_hash &key);

  void recvblk (bigint key, bool last);
};




#endif /* _MERKLE_SYNCER_H_ */
