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
  chord_node dst;

  void
  go ()
  {
    while (iter->more () && num_sends_pending < 64) {
      merkle_hash key = iter->next ();
      num_sends_pending++;
      XXX_SENDBLOCK_ARGS a (dst, tobigint (key), !iter->more(), 
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
  }

  merkle_send_range (db_iterator *iter, sndblkfnc2_t sndblkfnc, chord_node dst)
    : iter (iter), num_sends_pending (0), sndblkfnc (sndblkfnc), dst (dst)
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

merkle_server::merkle_server (merkle_tree *ltree, 
			      addHandler_t addHandler, 
			      sndblkfnc2_t sndblkfnc,
			      vnode *host_node)
  : ltree (ltree), sndblkfnc (sndblkfnc), host_node (host_node)
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
    rpcnode->child_hash.setsize (64);
    for (int i = 0; i < 64; i++)
      rpcnode->child_hash[i] = node->child (i)->hash;
  } else {
    vec<merkle_hash> keys = database_get_keys (ltree->db, depth, prefix);

    if (keys.size () != rpcnode->count) {
      warn << "\n\n\n----------------------------------------------------------\n";
      warn << "BUG BUG  BUG  BUG  BUG  BUG  BUG  BUG  BUG  BUG  BUG  BUG BUG\n";
      warn << "Send this output to cates@mit.edu\n";
      warn << "BUG: " << keys.size () << " != " << rpcnode->count << "\n";
      ltree->check_invariants ();
      warn << "BUG BUG  BUG  BUG  BUG  BUG  BUG  BUG  BUG  BUG  BUG  BUG BUG\n";
      panic << "----------------------------------------------------------\n\n\n";
    }

    rpcnode->child_hash.setsize (keys.size ());
    for (u_int i = 0; i < keys.size (); i++) {
      rpcnode->child_hash[i] = keys[i];
    }
  }
}


void
merkle_server::dispatch (svccb *sbp, void *args, int procno)
{

  if (!sbp)
    return;

  switch (procno) {
  case MERKLESYNC_GETNODE:
    // request a node of the merkle tree
    {
      //      getnode_arg *arg = sbp->template getarg<getnode_arg> ();
      getnode_arg *arg = (static_cast<getnode_arg *> (args));
      merkle_node *lnode;
      u_int lnode_depth;
      merkle_hash lnode_prefix;
      lnode = ltree->lookup (&lnode_depth, arg->depth, arg->prefix);
      lnode_prefix = arg->prefix;
      lnode_prefix.clear_suffix (lnode_depth);
      
      getnode_res res (MERKLE_OK);
      format_rpcnode (ltree, lnode_depth, lnode_prefix, lnode, &res.resok->node);
      host_node->doRPC_reply (sbp, &res, merklesync_program_1, 
			      MERKLESYNC_GETNODE);
      break;
    }
    

  case MERKLESYNC_GETBLOCKLIST:
    // request a list of blocks
    {
      //warn << (u_int) this << " dis..GETBLOCKLIST\n";	
      //      getblocklist_arg *arg = sbp->template getarg<getblocklist_arg> ();
      getblocklist_arg *arg = (static_cast<getblocklist_arg *> (args));
      ref<getblocklist_arg> arg_copy = New refcounted <getblocklist_arg> (*arg);
      getblocklist_res res (MERKLE_OK);
      chord_node src = arg->src;  // MUST be before sbp->reply
      host_node->doRPC_reply (sbp, &res, merklesync_program_1, 
			      MERKLESYNC_GETBLOCKLIST);

      
      // XXX DEADLOCK: if the blocks arrive before res, the remote side gets stuck.
      for (u_int i = 0; i < arg_copy->keys.size (); i++) {
	merkle_hash key = arg_copy->keys[i];
	bool last = (i + 1 == arg_copy->keys.size ());
	XXX_SENDBLOCK_ARGS a (src, tobigint (key), last, wrap (&ignorecb));
	(*sndblkfnc) (&a);
      }
      break;
    }
    
    
  case MERKLESYNC_GETBLOCKRANGE:
    // request all blocks in a given range, excluding a list of blocks (xkeys)
    {
      //warn << (u_int) this << " dis..GETBLOCKRANGE\n";	
      //      getblockrange_arg *arg = sbp->template getarg<getblockrange_arg> ();
      getblockrange_arg *arg = (static_cast<getblockrange_arg *> (args));
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
      chord_node src = arg->src;
      host_node->doRPC_reply (sbp, &res, merklesync_program_1, 
			      MERKLESYNC_GETBLOCKRANGE);


      // XXX DEADLOCK: if the blocks arrive before 'res'...remote gets stuck
      vNew merkle_send_range (iter, sndblkfnc, src);
      break;
    }
    
  default:
    warn << "uknown program in merkle " << procno << "\n";
    assert (0);
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}
