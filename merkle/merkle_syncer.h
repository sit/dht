#ifndef _MERKLE_SYNCER_H_
#define _MERKLE_SYNCER_H_

#include "merkle_hash.h"
#include "merkle_node.h"
#include "merkle_tree.h"
#include "merkle_sync_prot.h"
#include "bigint.h"

class db_iterator {
public:
  virtual bool more () = 0;
  virtual merkle_hash next () = 0;
  virtual merkle_hash peek () = 0;
};


class merkle_syncer {
public:
  typedef enum {
    BIDIRECTIONAL,
    UNIDIRECTIONAL
  } mode_t;

  mode_t mode;

  merkle_tree *ltree; // local tree
  ptr<asrv> srv;
  ptr<aclnt> clnt;
  timecb_t *tcb;
  cbv::ptr synccb;

  bigint rngmin;
  bigint rngmax;

  bool receiving_blocks;
  uint64 num_sends_pending;
  db_iterator *sendblocks_iter;

  vec<pair<merkle_rpc_node, int> > st;

  void dump ();
  merkle_syncer (merkle_tree *ltree, int fd);
  void dispatch (svccb *sbp);
  void send_some ();
  void next (void);
  void sendblock (merkle_hash key, bool last);
  void sendblock_cb (ref<sendblock_res> res, clnt_stat err);
  void getblocklist (vec<merkle_hash> keys);
  void getblocklist_cb (ref<getblocklist_res> res, clnt_stat err);



  void sync (cbv::ptr cb, mode_t m = BIDIRECTIONAL);
  void sync_range (bigint _leftinc, bigint _rightinc);
  void getnode (u_int depth, const merkle_hash &prefix);
  void getnode_cb (ref<getnode_arg> arg, ref<getnode_res> res, clnt_stat err);
  void getblockrange (u_int depth, const merkle_hash &prefix);
  void getblockrange_cb (ref<getblockrange_arg> arg, ref<getblockrange_res> res, clnt_stat err);

  void format_rpcnode (u_int depth, const merkle_hash &prefix,
		       const merkle_node *node, merkle_rpc_node *rpcnode);
  bool inrange (const merkle_hash &key);
};




#endif /* _MERKLE_SYNCER_H_ */
