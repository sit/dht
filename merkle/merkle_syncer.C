#include "merkle_syncer.h"
#include "qhash.h"
#include "async.h"
#include "bigint.h"
#include "chord_util.h"

#define MERKLE_SYNC_TRACE

// ---------------------------------------------------------------------------
// util junk


// Check whether [l1, r1] overlaps [l2, r2] on the circle.
static bool
overlap (const bigint &l1, const bigint &r1, const bigint &l2, const bigint &r2)
{
  // There might be a more efficient way to do this..
  return (betweenbothincl (l1, r1, l2) || betweenbothincl (l1, r1, r2)
	  || betweenbothincl (l2, r2, l1) || betweenbothincl (l2, r2, r1));
}


// ---------------------------------------------------------------------------
// merkle_syncer


merkle_syncer::merkle_syncer (merkle_tree *ltree, rpcfnc_t rpcfnc, missingfnc_t missingfnc)
  : ltree (ltree), rpcfnc (rpcfnc), missingfnc (missingfnc)
{
  fatal_err = NULL;
  sync_done = false;
}


void
merkle_syncer::sync (bigint _rngmin, bigint _rngmax)
{
  rngmin = _rngmin;
  rngmax = _rngmax;

  // start at the root of the merkle tree
  sendnode (0, 0);
}


void
merkle_syncer::dump ()
{    
  warn << "THIS: " << (u_int)this << "\n";
  warn << "  st.size () " << st.size () << "\n"; 
}


void
merkle_syncer::doRPC (int procno, ptr<void> in, void *out, aclnt_cb cb)
{
  // Must resort to bundling all values into one argument since
  // async/callback.h is configured with too few parameters.
  struct RPC_delay_args args (NULL, merklesync_program_1, procno, in, out, cb);
  (*rpcfnc) (&args);
}

void
merkle_syncer::setdone ()
{
  sync_done = true;
}


void
merkle_syncer::error (str err)
{
  warn << (u_int)this << ": SYNCER ERROR: " << err << "\n";
  fatal_err = err;
  setdone ();
}

str
merkle_syncer::getsummary ()
{
  assert (sync_done);
  strbuf sb;

  sb << "[" << rngmin << "," << rngmax << "] ";

  if (fatal_err)
    sb << fatal_err;

  if (0)
    sb  << "<log " << log << ">\n";

  return sb;
}

void
merkle_syncer::next (void)
{
  ///**/warn << (u_int)this << " next >>>>>>>>>>>>>>>>>>>>>>>>>> blks " << receiving_blocks << "\n";
  assert (!sync_done);
  assert (!fatal_err);

  while (st.size ()) {
    ///**/warn << "NEXT: SIZE != 0\n"; 
    
    pair<merkle_rpc_node, int> &p = st.back ();
    merkle_rpc_node *rnode = &p.first;
    assert (!rnode->isleaf);
    
    merkle_node *lnode = ltree->lookup_exact (rnode->depth, rnode->prefix);
    assert (lnode); // XXX fix this
    assert (!lnode->isleaf ()); // XXX fix this
    
    while (p.second < 64) {
      int i = p.second;
      ///**/warn << "CHECKING: i " << i << ", size " << st.size ()  << "\n"; 
      p.second += 1;
      if (rnode->child_hash[i] != lnode->child (i)->hash) {
	///**/warn << " * DIFFER: i " << i << ", size " << st.size ()  << "\n"; 
	unsigned int depth = rnode->depth + 1;
	merkle_hash prefix = rnode->prefix;
	prefix.write_slot (rnode->depth, i);
	bigint child_rngmin = tobigint (prefix);
	bigint child_range_width = bigint (1) << (160 - 6*depth);
	bigint child_rngmax = child_rngmin + child_range_width - 1;
	if (!overlap (rngmin, rngmax, child_rngmin, child_rngmax))
	  continue;
	sendnode (depth, prefix);
	return;
      } else {
	///**/warn << " * IDENTICAL: i " << i << ", size " << st.size ()  << "\n"; 
      }
    }
    
    assert (p.second == 64);
    st.pop_back ();
  }

  ///**/warn << "DONE .. in NEXT\n";
  setdone ();
  ///**/warn << "OK!\n";
}


void
merkle_syncer::sendnode (u_int depth, const merkle_hash &prefix)
{
  ref<sendnode_arg> arg = New refcounted<sendnode_arg> ();
  ref<sendnode_res> res = New refcounted<sendnode_res> ();

  u_int lnode_depth;
  merkle_node *lnode = ltree->lookup (&lnode_depth, depth, prefix);
  assert (lnode);
  assert (lnode_depth == depth);

  format_rpcnode (ltree, depth, prefix, lnode, &arg->node);
  doRPC (MERKLESYNC_SENDNODE, arg, res,
	 wrap (this, &merkle_syncer::sendnode_cb, arg, res));
}


void
merkle_syncer::sendnode_cb (ref<sendnode_arg> arg, ref<sendnode_res> res, 
			    clnt_stat err)
{
#ifdef MERKLE_SYNC_TRACE
  //  warn << (u_int)this << " sendnode_cb >>>>>>>>>>>>>>>>>>>>>>\n";
#endif
  if (err) {
    error (strbuf () << "SENDNODE: rpc error " << err);
    return;
  } else if (res->status != MERKLE_OK) {
    error (strbuf () << "SENDNODE: protocol error " << err2str (res->status));
    return;
  }

  merkle_rpc_node *rnode = res->node;
  assert (res->node->depth == arg->node.depth); // XXX relax this
  merkle_node *lnode = ltree->lookup_exact (rnode->depth, rnode->prefix);
  assert (lnode); // XXX fix this
  
  compare_nodes (ltree, rngmin, rngmax, lnode, rnode, missingfnc, rpcfnc);

  if (!lnode->isleaf () && !rnode->isleaf) {
#ifdef MERKLE_SYNC_TRACE
    warn << "I vs I\n";
#endif
    st.push_back (pair<merkle_rpc_node, int> (*rnode, 0));
  }

  next ();
}



// ---------------------------------------------------------------------------
// merkle_getkeyrange

void
merkle_getkeyrange::go ()
{
  if (current > rngmax) {
    warn << "merkle_getkeyrange::go () ==> DONE\n";
    delete this;
    return;
  }

  ref<getkeys_arg> arg = New refcounted<getkeys_arg> ();
  arg->rngmin = current;
  arg->rngmax = rngmax;
  ref<getkeys_res> res = New refcounted<getkeys_res> ();
  doRPC (MERKLESYNC_GETKEYS, arg, res,
	 wrap (this, &merkle_getkeyrange::getkeys_cb, arg, res));
}



void
merkle_getkeyrange::getkeys_cb (ref<getkeys_arg> arg, ref<getkeys_res> res, 
				clnt_stat err)

{
  if (err) {
    warn << "GETKEYS: rpc error " << err << "\n";
    delete this;
    return;
  } else if (res->status != MERKLE_OK) {
    warn << "GETKEYS: protocol error " << err2str (res->status) << "\n";
    delete this;
    return;
  }

  for (u_int i = 0; i < res->resok->keys.size (); i++) {
    const merkle_hash &key = res->resok->keys[i];
    bigint key2  = tobigint (key);
    if (!database_lookup (db, key))
      (*missing) (key2);
    if (key2 >= current)
      current = key2 + 1;
  }

  if (!res->resok->morekeys)
    current = rngmax + 1;  // set done

  go ();
}


void
merkle_getkeyrange::doRPC (int procno, ptr<void> in, void *out, aclnt_cb cb)
{
  // Must resort to bundling all values into one argument since
  // async/callback.h is configured with too few parameters.
  struct RPC_delay_args args (NULL, merklesync_program_1, procno, in, out, cb);
  (*rpcfnc) (&args);
}


// ---------------------------------------------------------------------------

void
compare_nodes (merkle_tree *ltree, bigint rngmin, bigint rngmax, 
	       merkle_node *lnode, merkle_rpc_node *rnode,
	       missingfnc_t missingfnc, rpcfnc_t rpcfnc)
{
#ifdef MERKLE_SYNC_TRACE
  warn << (lnode->isleaf ()  ? "L" : "I")
       << " vs "
       << (rnode->isleaf ? "L" : "I")
       << "\n";
#endif

  if (rnode->isleaf) {
    for (u_int i = 0; i < rnode->child_hash.size (); i++) {
      merkle_hash key = rnode->child_hash[i];
      if (betweenbothincl (rngmin, rngmax, tobigint (key)))
	if (database_lookup (ltree->db, key) == NULL)
	  (*missingfnc) (tobigint (key));
    }
  } else if (lnode->isleaf () && !rnode->isleaf) {
    bigint node_rngmin = tobigint (rnode->prefix);
    bigint node_range_size = bigint (1) << (160 - rnode->depth);
    bigint node_rngmax = node_rngmin + node_range_size - 1;
    vNew merkle_getkeyrange (ltree->db, node_rngmin, node_rngmax, missingfnc, rpcfnc);
  }
}

// ---------------------------------------------------------------------------

void
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
