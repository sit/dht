#ifndef _MERKLE_SERVER_H_
#define _MERKLE_SERVER_H_

#include "merkle_hash.h"
#include "merkle_node.h"
#include "merkle_tree.h"
#include "merkle_syncer.h"
#include "merkle_sync_prot.h"
#include "bigint.h"
#include "qhash.h"
#include <chord.h>

class RPC_delay_args;

typedef callback<void, const rpc_program &, cbdispatch_t>::ref addHandler_t;
typedef callback<void, ptr<location>, blockID>::ref missingfnc2_t;

// One merkle_server runs for each node of the Chord ring.
//  - i.e., one merkle_server per virtual node
//  - and remember, each virtual node has own database 

class merkle_server {
 public:
  merkle_tree *ltree; // local tree
  vnode *host_node;  // XXX bad -- don't directly rely on dhash!
  missingfnc2_t missingfnc;

  void missing (ptr<location> n, blockID key);
  void doRPC (ptr<location> n, RPC_delay_args *args);
  void dispatch (user_args *a);
  merkle_server (merkle_tree *ltree, addHandler_t addHandler, 
		 missingfnc2_t missingfnc, vnode *host_node);
};


#endif /* _MERKLE_SERVER_H_ */
