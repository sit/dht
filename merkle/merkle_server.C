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
	// FED: used to be an assert, now a warning. Think code handles
	// this case.
	warn << "local depth ( " << lnode_depth 
	     << ") is not equal to remote depth (" << rnode->depth << ")\n";
	warn << "prefix is " << rnode->prefix << "\n";
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
