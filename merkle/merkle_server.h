#ifndef _MERKLE_SERVER_H_
#define _MERKLE_SERVER_H_

#include "merkle_hash.h"
#include "merkle_node.h"
#include "merkle_tree.h"
#include "merkle_sync_prot.h"
#include "merkle_syncer.h"
#include "bigint.h"
#include "qhash.h"
#include "chord.h"
#include "dhash.h"

class XXX_SENDBLOCK_ARGS;

// the pair is because of the limit on the max # of args in a wrap 
typedef callback<void, XXX_SENDBLOCK_ARGS *>::ref sndblkfnc2_t;

// One merkle_server runs for each node of the Chord ring.
//
// Think about virtual nodes:
//  - one merkle_server per virtual node
//  - remember, each vnode has own database 


class merkle_server {
public:
  merkle_tree *ltree; // local tree
  vnode *host_node;
  sndblkfnc2_t sndblkfnc;

  // maps sync session id -> a syncer
  // (to dispatch incoming blocks to the target syncer)
  qhash<u_int, merkle_syncer> syncers;

  // called when dhash receives a new block
  // void handle_block (...);

  merkle_server (merkle_tree *ltree, vnode *host_node, sndblkfnc2_t sndblkfnc);
  void dispatch (svccb *sbp);
};




#endif /* _MERKLE_SERVER_H_ */
