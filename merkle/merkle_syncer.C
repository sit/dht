#include <chord.h>
#include "merkle_syncer.h"
#include "qhash.h"
#include "async.h"
#include "bigint.h"
#include <id_utils.h>
#include <comm.h>

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
  struct RPC_delay_args args (merklesync_program_1, procno, in, out, cb, 
			      NULL);
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
  warn << "local range [" <<  local_rngmin << "," << local_rngmax << "]\n";
#endif
  assert (!sync_done);
  assert (!fatal_err);

  // st is queue of pending index nodes
  while (st.size ()) {
    pair<merkle_rpc_node, int> &p = st.back ();
    merkle_rpc_node *rnode = &p.first;
    assert (!rnode->isleaf);
    
    merkle_node *lnode = ltree->lookup_exact (rnode->depth, rnode->prefix);

    if (!lnode) {
      fatal << "lookup_exact didn't match for " << rnode->prefix << " at depth " << rnode->depth << "\n";
    }
    

#ifdef MERKLE_SYNC_DETAILED_TRACE
    warn << "starting from slot " << p.second << "\n";
#endif

    // XXX not clear p.second will ever enter > 0
    while (p.second < 64) {
      u_int i = p.second;
      p.second += 1;
#ifdef MERKLE_SYNC_DETAILED_TRACE
      warn << "CHECKING: " << i;
#endif

      bigint remote = tobigint (rnode->child_hash[i]);
      bigint local = tobigint (lnode->child (i)->hash);

      u_int depth = rnode->depth + 1;

      //prefix is the high bits of the first key 
      // the node is responsible for.
      merkle_hash prefix = rnode->prefix;
      prefix.write_slot (rnode->depth, i);
      bigint slot_rngmin = tobigint (prefix);
      bigint slot_width = bigint (1) << (160 - 6*depth);
      bigint slot_rngmax = slot_rngmin + slot_width - 1;

      bool overlaps = overlap (local_rngmin, local_rngmax, slot_rngmin, slot_rngmax);

      if (remote != local) {
#ifdef MERKLE_SYNC_DETAILED_TRACE
	warnx << " differ. local " << local << " != remote " << remote;
#endif
	if (overlaps) {
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

  ref<sendnode_arg> arg = New refcounted<sendnode_arg> ();
  ref<sendnode_res> res = New refcounted<sendnode_res> ();

  u_int lnode_depth;
  merkle_node *lnode = ltree->lookup (&lnode_depth, depth, prefix);
  assert (lnode);
  assert (lnode_depth == depth);

  format_rpcnode (ltree, depth, prefix, lnode, &arg->node);
  arg->rngmin = local_rngmin;
  arg->rngmax = local_rngmax;
  doRPC (MERKLESYNC_SENDNODE, arg, res,
	 wrap (this, &merkle_syncer::sendnode_cb, arg, res));
}


void
merkle_syncer::sendnode_cb (ref<sendnode_arg> arg, ref<sendnode_res> res, 
			    clnt_stat err)
{

  if (err) {
    error (strbuf () << "SENDNODE: rpc error " << err);
    return;
  } else if (res->status != MERKLE_OK) {
    error (strbuf () << "SENDNODE: protocol error " << err2str (res->status));
    return;
  }

  merkle_rpc_node *rnode = &res->resok->node;

  merkle_node *lnode = ltree->lookup_exact (rnode->depth, rnode->prefix);
  if (!lnode) {
    fatal << "lookup failed: " << rnode->prefix << " at " << rnode->depth << "\n";
  }
  
  compare_nodes (ltree, local_rngmin, local_rngmax, lnode, rnode, 
		 missingfnc, rpcfnc);

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
  if (between (rngmax, incID (current), current)) {
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

  vec<merkle_hash> rkeys;
  chordID round_max = current;
  chordID next_cur = current;
  for (u_int i = 0; i < res->resok->keys.size (); i++) {
    const merkle_hash &key = res->resok->keys[i];
    rkeys.push_back (key);

    bigint bkey = tobigint(key);
    if (round_max < bkey) round_max = bkey;
    if (betweenbothincl (next_cur, incID (bkey), bkey))
      next_cur = incID (bkey);
  }

  compare_keylists (lkeys, rkeys, current, round_max, missing);

  current = next_cur;
  
  if (!res->resok->morekeys)
    current = incID (rngmax);  // set done
  
  go ();
}


void
merkle_getkeyrange::doRPC (int procno, ptr<void> in, void *out, aclnt_cb cb)
{
  // Must resort to bundling all values into one argument since
  // async/callback.h is configured with too few parameters.
  struct RPC_delay_args args (merklesync_program_1, procno, in, out, cb,
			      NULL);
  (*rpcfnc) (&args);
}


// ---------------------------------------------------------------------------

void
compare_keylists (vec<merkle_hash> lkeys,
		  vec<merkle_hash> vrkeys,
		  chordID rngmin, chordID rngmax,
		  missingfnc_t missingfnc)
{
  // populate a hash table with the remote keys
  qhash<merkle_hash, int> rkeys;
  for (u_int i = 0; i < vrkeys.size (); i++) {
    if (betweenbothincl (rngmin, rngmax, tobigint(vrkeys[i]))) 
      rkeys.insert (vrkeys[i], 1);
  }
    
  // do I have something he doesn't have?
  for (unsigned int i = 0; i < lkeys.size (); i++) {
    if (!rkeys[lkeys[i]]) {
#ifdef MERKLE_SYNC_DETAILED_TRACE
    warn << "remote missing [" << rngmin << ", " << rngmax << "] key=" << lkeys[i] << "\n";
#endif 
      (*missingfnc) (tobigint(lkeys[i]), false);
    } else {
      rkeys.remove (lkeys[i]);
    }
  }

  //anything left: he has and I don't
  qhash_slot<merkle_hash, int> *slot = rkeys.first ();
  while (slot) {
#ifdef MERKLE_SYNC_DETAILED_TRACE
    warn << "local missing [" << rngmin << ", " << rngmax << "] key=" << slot->key << "\n";
#endif 
    (*missingfnc) (tobigint(slot->key), true);
    slot = rkeys.next (slot);
  }
   
}

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
    vec<merkle_hash> lkeys = database_get_keys (ltree->db, rnode->depth, rnode->prefix);

    vec<merkle_hash> rkeys;
    for (u_int i = 0; i < rnode->child_hash.size (); i++) {
      if (betweenbothincl (rngmin, rngmax, tobigint (rnode->child_hash[i])))
	rkeys.push_back (rnode->child_hash[i]);
    }

    compare_keylists (lkeys, rkeys, rngmin, rngmax, missingfnc);    
	
  } else if (lnode->isleaf () && !rnode->isleaf) {
    bigint tmpmin = tobigint (rnode->prefix);
    bigint node_width = bigint (1) << (160 - rnode->depth);
    bigint tmpmax = tmpmin + node_width - 1;


    // further constrain to be within the host's range of interest
    if (between (tmpmin, tmpmax, rngmin))
      tmpmin = rngmin;
    if (between (tmpmin, tmpmax, rngmax))
      tmpmax = rngmax;

    vec<merkle_hash> lkeys = database_get_keys (ltree->db, rnode->depth, rnode->prefix);
    vNew merkle_getkeyrange (ltree->db, tmpmin, tmpmax, lkeys, missingfnc, rpcfnc);
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
      warn << "Send this output to chord@pdos.lcs.mit.edu\n";
      warn << "BUG: " << depth << " " << prefix << "\n";
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
