#include "merkle_server.h"
#include <comm.h>
#include "transport_prot.h"

// ---------------------------------------------------------------------------
// merkle_server

merkle_server::merkle_server (merkle_tree *ltree, addHandler_t addHandler, 
			      missingfnc2_t missingfnc, vnode *host_node)
  : ltree (ltree), host_node (host_node), missingfnc (missingfnc)
{
  (*addHandler) (merklesync_program_1, wrap(this, &merkle_server::dispatch));
}


void
merkle_server::missing (chord_node n, bigint key)
{
  (*missingfnc) (n, key);
}

void
merkle_server::doRPC (chord_node dst, RPC_delay_args *args)
{
  host_node->doRPC (dst, args->prog, args->procno, args->in, args->out, args->cb);
}

void
merkle_server::dispatch (svccb *sbp, void *args, int procno)
{
  if (!sbp)
    return;

  switch (procno) {
  case MERKLESYNC_SENDNODE:
    // request a node of the merkle tree
    {
      sendnode_arg *arg = (static_cast<sendnode_arg *> (args));
      merkle_rpc_node *rnode = &arg->node;
      merkle_node *lnode;
      u_int lnode_depth;
      merkle_hash lnode_prefix;
      lnode = ltree->lookup (&lnode_depth, rnode->depth, rnode->prefix);
      assert (lnode_depth == rnode->depth);
      lnode_prefix = rnode->prefix;
      // XXX isn't this a NOP, given the previous assertion about equal depths 
      lnode_prefix.clear_suffix (lnode_depth);

      // Get remote sides ip:port and chordID
      dorpc_arg *t_arg = sbp->template getarg<dorpc_arg> ();
      const struct sockaddr_in *sa = (struct sockaddr_in *)sbp->getsa ();
      chord_node from;
      from.r.hostname = inet_ntoa (sa->sin_addr);
      from.r.port = ntohs (sa->sin_port);
      from.x = t_arg->src_id;

      bigint rngmin = 0; //host_node->my_pred ();
      bigint rngmax = bigint (1) << 160; //host_node->my_ID ();
      //bigint rngmin = host_node->my_pred ();
      //bigint rngmax = host_node->my_ID ();

      compare_nodes (ltree, rngmin, rngmax, lnode, rnode,
		     wrap (this, &merkle_server::missing, from),
		     wrap (this, &merkle_server::doRPC, from));

      sendnode_res res (MERKLE_OK);
      format_rpcnode (ltree, lnode_depth, lnode_prefix, lnode, res.node);
      host_node->doRPC_reply (sbp, &res, merklesync_program_1, 
			      MERKLESYNC_SENDNODE);
      break;
    }
     
  default:
    fatal << "unknown proc in merkle " << procno << "\n";
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}
