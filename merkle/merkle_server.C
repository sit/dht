#include "merkle_server.h"

// ---------------------------------------------------------------------------
// merkle_send_range

// This class
//     1) sends all the keys returned by the database iterator
//     2) and then deletes itself
// It limits itself to at max 64 outstanding sends.

class merkle_send_range {
private:
  db_iterator *iter;
  u_int num_sends_pending;
  sndblkfnc2_t sndblkfnc;
  chordID dstID;

  void
  go ()
  {
    while (iter->more () && num_sends_pending < 64) {
      merkle_hash key = iter->next ();
      num_sends_pending++;
      XXX_SENDBLOCK_ARGS a (dstID, tobigint (key), !iter->more(), 
			    wrap (this, &merkle_send_range::sendcb));
      (*sndblkfnc) (&a);
    }
    
    if (!iter->more () && num_sends_pending == 0)
      delete this;      
  }

  void
  sendcb ()
  {
    num_sends_pending--;
    go ();
  }


public:
  ~merkle_send_range ()
  {
    warn << (u_int)this << " DTOR merkle_send_range\n";
  }

  merkle_send_range (db_iterator *iter, sndblkfnc2_t sndblkfnc, chordID dstID)
    : iter (iter), num_sends_pending (0), sndblkfnc (sndblkfnc), dstID (dstID)
  {
    go ();
  }
};


// ---------------------------------------------------------------------------
// util junk

// XXX CODE DUPLICATED IN MERKLE_SYNCER.C
static qhash<merkle_hash, bool> *
make_set (rpc_vec<merkle_hash, 64> &v)
{
  qhash<merkle_hash, bool> *s = New qhash<merkle_hash, bool> ();
  for (u_int i = 0; i < v.size (); i++)
    s->insert (v[i], true);
  return s;
}

static void
ignorecb ()
{
}

// ---------------------------------------------------------------------------
// merkle_server

merkle_server::merkle_server (merkle_tree *ltree, addHandler_t addHandler, sndblkfnc2_t sndblkfnc)
  : ltree (ltree), sndblkfnc (sndblkfnc)
{
  (*addHandler) (merklesync_program_1, wrap(this, &merkle_server::dispatch));
}


static void
format_rpcnode (merkle_tree *ltree, u_int depth, const merkle_hash &prefix,
		const merkle_node *node, merkle_rpc_node *rpcnode)
{
  rpcnode->depth = depth;
  rpcnode->prefix = prefix;
  rpcnode->count = node->count;
  rpcnode->hash = node->hash;
  rpcnode->isleaf = node->isleaf ();
  
  if (!node->isleaf ()) {
    rpcnode->child_isleaf.setsize (64);
    rpcnode->child_hash.setsize (64);
    
    for (int i = 0; i < 64; i++) {
      const merkle_node *child = node->child (i);
      rpcnode->child_isleaf[i] = child->isleaf ();
      rpcnode->child_hash[i] = child->hash;
    }
  } else {
    vec<merkle_hash> keys = database_get_keys (ltree->db, depth, prefix);

    assert (keys.size () == rpcnode->count);

    rpcnode->child_hash.setsize (keys.size ());
    for (u_int i = 0; i < keys.size (); i++) {
      rpcnode->child_hash[i] = keys[i];
    }
  }
}


void
merkle_server::dispatch (svccb *sbp)
{

  if (!sbp)
    return;

  switch (sbp->proc ()) {
  case MERKLESYNC_GETNODE:
    // request a node of the merkle tree
    {
      getnode_arg *arg = sbp->template getarg<getnode_arg> ();
      merkle_node *lnode;
      u_int lnode_depth;
      merkle_hash lnode_prefix;
      lnode = ltree->lookup (&lnode_depth, arg->depth, arg->prefix);
      lnode_prefix = arg->prefix;
      lnode_prefix.clear_suffix (lnode_depth);
      
      getnode_res res (MERKLE_OK);
      format_rpcnode (ltree, lnode_depth, lnode_prefix, lnode, &res.resok->node);
      sbp->reply (&res);
      break;
    }
    

  case MERKLESYNC_GETBLOCKLIST:
    // request a list of blocks
    {
      //warn << (u_int) this << " dis..GETBLOCKLIST\n";	
      getblocklist_arg *arg = sbp->template getarg<getblocklist_arg> ();
      ref<getblocklist_arg> arg_copy = New refcounted <getblocklist_arg> (*arg);
      getblocklist_res res (MERKLE_OK);
      chordID srcID = arg->srcID;  // MUST be before sbp->reply
      sbp->reply (&res);
      
      // XXX DEADLOCK: if the blocks arrive before res, the remote side gets stuck.
      for (u_int i = 0; i < arg_copy->keys.size (); i++) {
	merkle_hash key = arg_copy->keys[i];
	bool last = (i + 1 == arg_copy->keys.size ());
	XXX_SENDBLOCK_ARGS a (srcID, tobigint (key), last, wrap (&ignorecb));
	(*sndblkfnc) (&a);
      }
      break;
    }
    
    
  case MERKLESYNC_GETBLOCKRANGE:
    // request all blocks in a given range, excluding a list of blocks (xkeys)
    {
      //warn << (u_int) this << " dis..GETBLOCKRANGE\n";	
      getblockrange_arg *arg = sbp->template getarg<getblockrange_arg> ();
      getblockrange_res res (MERKLE_OK);
      
      if (arg->bidirectional) {
	res.resok->desired_xkeys.setsize (arg->xkeys.size ());
	for (u_int i = 0; i < arg->xkeys.size (); i++) {
	  ptr<dbrec> blk = database_lookup (ltree->db, arg->xkeys[i]);
	  res.resok->desired_xkeys[i] = (blk == NULL);
	}
      }

      db_iterator *iter;
      iter = New db_range_xiterator (ltree->db, arg->depth, arg->prefix,
				     make_set (arg->xkeys), arg->rngmin,
				     arg->rngmax);
      res.resok->will_send_blocks = iter->more ();
      bigint srcID = arg->srcID;
      sbp->reply (&res);  // DONT REF arg AFTER THIS POINT!!!!

      // XXX DEADLOCK: if the blocks arrive before 'res'...remote gets stuck
      vNew merkle_send_range (iter, sndblkfnc, srcID);
      break;
    }
    
  default:
    assert (0);
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}
