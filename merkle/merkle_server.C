#include <chord.h>
#include "merkle_hash.h"
#include "merkle_node.h"
#include "merkle_tree.h"
#include "merkle_syncer.h"
#include "merkle_sync_prot.h"
#include "merkle_server.h"
#include <location.h>
#include <locationtable.h>
#include <comm.h>

// ---------------------------------------------------------------------------
// merkle_server

merkle_server::merkle_server (ptr<merkle_tree> ltree) : ltree (ltree)
{
}

void
merkle_server::handle_get_keys (getkeys_arg *arg, getkeys_res *res)
{
  //get the first 65 keys in the range. We'll return 64, but we get
  // 65 so we can tell if we got them all
  vec<chordID> keys = ltree->get_keyrange (arg->rngmin, arg->rngmax, 65);
  
  warn << "get keys (" << arg->rngmin << ", " << arg->rngmax << " returning " << keys.size () << " keys\n";
  bool more = (keys.size () == 65); // there is at least one more key to get
  
  res->resok->morekeys = more;
  //trim off the extra key if necessary
  while (keys.size () > 64) keys.pop_back ();
  res->resok->keys = keys;
}

void
merkle_server::handle_send_node (sendnode_arg *arg, sendnode_res *res)
{
  merkle_rpc_node *rnode = &arg->node;
  merkle_node *lnode;
  u_int lnode_depth;
  merkle_hash lnode_prefix;
  lnode = ltree->lookup (&lnode_depth, rnode->depth, rnode->prefix);
  if (lnode_depth != rnode->depth) {
    warn << "local depth ( " << lnode_depth 
	 << ") is not equal to remote depth (" << rnode->depth << ")\n";
    warn << "prefix is " << rnode->prefix << "\n";
    //a remote node will only ask us for children if we previously returned
    // an index node at depth - 1. If local depth < remote depth, that
    // implies that we don't have an index node at depth - 1: assert!
    assert (lnode_depth == rnode->depth);
  }
  lnode_prefix = rnode->prefix;
  // XXX isn't this a NOP, given the previous assertion about equal depths 
  lnode_prefix.clear_suffix (lnode_depth);
  format_rpcnode (ltree, lnode_depth, lnode_prefix, 
		  lnode, &res->resok->node);
  // and reply
  ltree->lookup_release(lnode);
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
      sendnode_arg *arg = sbp->Xtmpl getarg<sendnode_arg> ();
      sendnode_res res (MERKLE_OK);
      handle_send_node (arg, &res);
      sbp->reply (&res);
      break;
    }
     
  case MERKLESYNC_GETKEYS:
    {
      getkeys_arg *arg = sbp->Xtmpl getarg<getkeys_arg> ();
      getkeys_res res (MERKLE_OK);
      handle_get_keys (arg, &res);
      sbp->reply (&res);
      break;
    }
  default:
    fatal << "unknown proc in merkle " << sbp->procno << "\n";
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}
