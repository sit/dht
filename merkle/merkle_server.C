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
#include "libadb.h"

// ---------------------------------------------------------------------------
// merkle_server

merkle_server::merkle_server (merkle_tree *ltree) : ltree (ltree)
{
}

void
merkle_server::dispatch (user_args *sbp)
{
  if (!sbp)
    return;


  //NNN choose the right merkle_server object based on a field of the RPC?

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

      sendnode_res res (MERKLE_OK);
      format_rpcnode (ltree, lnode_depth, lnode_prefix, 
		      lnode, &res.resok->node);
      // and reply
      sbp->reply (&res);
      break;
    }
     
  case MERKLESYNC_GETKEYS:
    {
      getkeys_arg *arg = sbp->template getarg<getkeys_arg> ();

      ltree->db->getkeys (arg->rngmin, 
			  wrap (this, &merkle_server::getkeys_cb,
				sbp, arg));
      break;
    }
  default:
    fatal << "unknown proc in merkle " << sbp->procno << "\n";
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}


void
merkle_server::getkeys_cb (user_args *sbp, getkeys_arg *arg, adb_status stat, 
			   vec<chordID> dbkeys)
{
  
  vec<chordID> keys;
  for (u_int i = 0; i < dbkeys.size () && i < 64; i++) {
    if (!betweenbothincl (arg->rngmin, arg->rngmax, dbkeys[i]))
      break;
    keys.push_back (dbkeys[i]);
  }
  
  bool more =  (keys.size () == 64 && // we took all we could get
		dbkeys.size () > 64 && // there are more to get
		// and they are potentially interesting
		betweenbothincl (arg->rngmin, arg->rngmax, dbkeys[64]));
    
  getkeys_res res (MERKLE_OK);
  res.resok->morekeys = more;
  res.resok->keys.setsize (keys.size ());
  for (u_int i = 0; i < keys.size (); i++)
    res.resok->keys[i] = keys[i];
  
  sbp->reply (&res);
}

