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
merkle_server::dispatch (user_args *sbp)
{
  if (!sbp)
    return;

  switch (sbp->procno) {
  case MERKLESYNC_SENDNODE:
    // request a node of the merkle tree
    {
      sendnode_arg *arg = sbp->template getarg<sendnode_arg> ();
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
      chord_node from;
      sbp->fill_from (&from);

      compare_nodes (ltree, arg->rngmin, arg->rngmax, lnode, rnode,
		     wrap (this, &merkle_server::missing, from),
		     wrap (this, &merkle_server::doRPC, from));

      sendnode_res res (MERKLE_OK);
      format_rpcnode (ltree, lnode_depth, lnode_prefix, lnode, &res.resok->node);
      sbp->reply (&res);
      break;
    }
     
  case MERKLESYNC_GETKEYS:
    {
      getkeys_arg *arg = sbp->template getarg<getkeys_arg> ();

      ptr<dbPair> d;
      vec<ptr<dbrec> > keys;
      ptr<dbEnumeration> enumer = ltree->db->enumerate ();
      for (u_int i = 0; i < 64; i++) {
	if (i == 0)
	  d = enumer->nextElement (id2dbrec(arg->rngmin)); // off-by-one??? 
	else
	  d = enumer->nextElement ();

	if (!d || dbrec2id(d->key) > arg->rngmax)
	  break;
	keys.push_back (d->key);
      }
      
      bool more = false;
      if (keys.size () == 64) {
	d = enumer->nextElement ();
	if (d && dbrec2id(d->key) <= arg->rngmax)
	  more = true;
      }

      getkeys_res res (MERKLE_OK);
      res.resok->morekeys = more;
      res.resok->keys.setsize (keys.size ());
      for (u_int i = 0; i < keys.size (); i++)
	res.resok->keys[i] = to_merkle_hash (keys[i]);

      sbp->reply (&res);
      break;
    }

  default:
    fatal << "unknown proc in merkle " << sbp->procno << "\n";
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}
