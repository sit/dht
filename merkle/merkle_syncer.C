#include "merkle_syncer.h"
#include "qhash.h"
#include "async.h"
#include "bigint.h"
#include "chord_util.h"

//#define MERKLE_SYNC_TRACE
//#define MERKLE_SYNC_DETAILED_TRACE

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
merkle_syncer::sync (bigint rngmin, bigint rngmax)
{
  local_rngmin = rngmin;
  local_rngmax = rngmax;

  remote_rngmin = 0;
  remote_rngmax = (bigint (1) << 160) - 1;

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

  sb << "[" << local_rngmin << "," << local_rngmax << "] ";
  sb << "[" << remote_rngmin << "," << remote_rngmax << "] ";

  if (fatal_err)
    sb << fatal_err;

  if (0)
    sb  << "<log " << log << ">\n";

  return sb;
}

void
merkle_syncer::next (void)
{
#ifdef MERKLE_SYNC_DETAILED_TRACE
  warn << (u_int)this << " next >>>>>>>>>>>>>>>>>>>>>>>>>>: st.size " << st.size () << "\n";
  warn << "local range [" <<  local_rngmin << "," << local_rngmax << "]\n";
  warn << "remote range [" <<  remote_rngmin << "," << remote_rngmax << "]\n";
#endif
  assert (!sync_done);
  assert (!fatal_err);

  while (st.size ()) {
    pair<merkle_rpc_node, int> &p = st.back ();
    merkle_rpc_node *rnode = &p.first;
    assert (!rnode->isleaf);
    
    merkle_node *lnode = ltree->lookup_exact (rnode->depth, rnode->prefix);
    assert (lnode); // XXX fix this
    assert (!lnode->isleaf ()); // XXX fix this
    
#ifdef MERKLE_SYNC_DETAILED_TRACE
    warn << "starting from slot " << p.second << "\n";
#endif
    while (p.second < 64) {
      u_int i = p.second;
      p.second += 1;
#ifdef MERKLE_SYNC_DETAILED_TRACE
      warn << "CHECKING: " << i;
#endif
      bigint remote = tobigint (rnode->child_hash[i]);
      bigint local = tobigint (lnode->child (i)->hash);

      u_int depth = rnode->depth + 1;
      merkle_hash prefix = rnode->prefix;
      prefix.write_slot (rnode->depth, i);
      bigint slot_rngmin = tobigint (prefix);
      bigint slot_width = bigint (1) << (160 - 6*depth);
      bigint slot_rngmax = slot_rngmin + slot_width - 1;

      bool slot_in_local_range = overlap (local_rngmin, local_rngmax, slot_rngmin, slot_rngmax);
      bool slot_in_remote_range = overlap (remote_rngmin, remote_rngmax, slot_rngmin, slot_rngmax);

      if (remote != local) {
#ifdef MERKLE_SYNC_DETAILED_TRACE
	warnx << " differ. local " << local << " != remote " << remote;
#endif
	if (( (remote != 0) && slot_in_local_range)
	    || ((local != 0) && slot_in_remote_range)) {
#ifdef MERKLE_SYNC_DETAILED_TRACE
	  warnx << " .. sending\n";
#endif
	  sendnode (depth, prefix);
	  return;
	} else {
#ifdef MERKLE_SYNC_DETAILED_TRACE
	  warnx << " .. not sending\n";
#endif
	}
      } else {
#ifdef MERKLE_SYNC_DETAILED_TRACE
	warnx << " same. local " << local << " == remote " << remote << "\n";
#endif
      }
    }
    
    assert (p.second == 64);
    st.pop_back ();
  }

#ifdef MERKLE_SYNC_DETAILED_TRACE
  warn << "DONE .. in NEXT\n";
#endif
  setdone ();
#ifdef MERKLE_SYNC_DETAILED_TRACE
  warn << "OK!\n";
#endif
}


void
merkle_syncer::sendnode (u_int depth, const merkle_hash &prefix)
{
#ifdef MERKLE_SYNC_TRACE
  warn << (u_int)this << " sendnode >>>>>>>>>>>>>>>>>>>>>>\n";
#endif

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
  warn << (u_int)this << " sendnode_cb >>>>>>>>>>>>>>>>>>>>>>\n";
#endif
  if (err) {
    error (strbuf () << "SENDNODE: rpc error " << err);
    return;
  } else if (res->status != MERKLE_OK) {
    error (strbuf () << "SENDNODE: protocol error " << err2str (res->status));
    return;
  }


  merkle_rpc_node *rnode = &res->resok->node;
  assert (rnode->depth == arg->node.depth); // XXX relax this

  remote_rngmin = res->resok->rngmin;
  remote_rngmax = res->resok->rngmax;

  merkle_node *lnode = ltree->lookup_exact (rnode->depth, rnode->prefix);
  assert (lnode); // XXX fix this
  
  compare_nodes (ltree, local_rngmin, local_rngmax, lnode, rnode, missingfnc, rpcfnc);

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
#ifdef MERKLE_SYNC_TRACE
    warn << "merkle_getkeyrange::go () ==> DONE\n";
#endif
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
    if (!database_lookup (db, key)) {
      warn << "2 [" << rngmin << "," << rngmax << "] => missing key "  << key << "\n";
      (*missing) (key2);
    }
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
	if (database_lookup (ltree->db, key) == NULL) {
	  warn << "1 [" << rngmin << "," << rngmax << "] => missing key "  << key << "\n";
	  (*missingfnc) (tobigint (key));
	}
    }
  } else if (lnode->isleaf () && !rnode->isleaf) {
    bigint tmpmin = tobigint (rnode->prefix);
    bigint node_width = bigint (1) << (160 - rnode->depth);
    bigint tmpmax = tmpmin + node_width - 1;

    // further constrain to be within the host's range of interest
    if (between (tmpmin, tmpmax, rngmin))
      tmpmin = rngmin;
    if (between (tmpmin, tmpmax, rngmax))
      tmpmax = rngmax;
    vNew merkle_getkeyrange (ltree->db, tmpmin, tmpmax, missingfnc, rpcfnc);
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
