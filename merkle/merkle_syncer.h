#ifndef _MERKLE_SYNCER_H_
#define _MERKLE_SYNCER_H_

#include "merkle_hash.h"
#include "merkle_tree.h"
#include "merkle_sync_prot.h"
#include <bigint.h>

template <class T1, class T2>
struct pair {
  T1 first;
  T2 second;
  pair (T1 f, T2 s) : first (f), second (s) {}
};

// see comment in merkle_syncer::doRPC for this work around.
struct RPC_delay_args;
typedef callback<void, RPC_delay_args *>::ref rpcfnc_t;
typedef callback<void, bigint, bool>::ref missingfnc_t;

class merkle_syncer : public virtual refcount {
 private:
  ptr<bool> deleted;

  uint vnode;
  dhash_ctype ctype;
  ptr<merkle_tree> ltree; // local tree
  rpcfnc_t rpcfnc;
  missingfnc_t missingfnc;

  str fatal_err;
  bool sync_done;

  bigint local_rngmin;
  bigint local_rngmax;

  bigint remote_rngmin;
  bigint remote_rngmax;

  vec<pair<merkle_rpc_node, int> > st;

  cbi completecb;

  void setdone ();
  void error (str err);
  void missing (const merkle_hash &key);
  void sendnode_cb (ptr<bool> deleted,
                    ref<sendnode_arg> arg, ref<sendnode_res> res, 
		    clnt_stat err);
  void compare_nodes (bigint rngmin, bigint rngmax,
      merkle_node *lnode, merkle_rpc_node *rnode);

  unsigned int outstanding_sendnodes;
  unsigned int outstanding_keyranges;
  void collect_keyranges (ptr<bool> deleted);

 public:
  merkle_syncer (uint vnode, dhash_ctype ctype,
		 ptr<merkle_tree> ltree, rpcfnc_t rpcfnc, 
		 missingfnc_t missingfnc);
  ~merkle_syncer ();

  void dump ();
  str getsummary ();
  void doRPC (int procno, ptr<void> in, void *out, aclnt_cb cb);
  void next (void);

  bool done () { return sync_done; }
  void sync (bigint rngmin, bigint rngmax, cbi cb = cbi_null);
  void sendnode (u_int depth, const merkle_hash &prefix);
};

void
format_rpcnode (merkle_tree *ltree, u_int depth, const merkle_hash &prefix,
		merkle_node *node, merkle_rpc_node *rpcnode);

#endif /* _MERKLE_SYNCER_H_ */
