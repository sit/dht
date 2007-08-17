#include <chord.h>
#include "merkle_syncer.h"
#include "qhash.h"
#include "async.h"
#include <id_utils.h>
#include <comm.h>

#include <modlogger.h>
#define warning modlogger ("merkle", modlogger::WARNING)
#define info  modlogger ("merkle", modlogger::INFO)
#define trace modlogger ("merkle", modlogger::TRACE)

// {{{ General utility functions
inline const strbuf &
strbuf_cat (const strbuf &sb, merkle_stat status)
{
  return rpc_print (sb, status, 0, NULL, NULL);
}

void
format_rpcnode (merkle_tree *ltree, u_int depth, const merkle_hash &prefix,
		merkle_node *node, merkle_rpc_node *rpcnode)
{
  rpcnode->depth = depth;
  rpcnode->prefix = prefix;
  rpcnode->count = node->count;
  rpcnode->hash = node->hash;
  rpcnode->isleaf = node->isleaf ();

  if (!node->isleaf ()) {
    rpcnode->child_hash.setsize (64);
    for (int i = 0; i < 64; i++)
      rpcnode->child_hash[i] = node->child_hash (i);
  } else {
    vec<merkle_hash> keys = ltree->database_get_keys (depth, prefix);

    if (keys.size () != rpcnode->count) {
      // This means that the tree has changed since the lookup.
      // (Perhaps a key was added or expired.)
      // Don't panic; just fudge it.
      warn << "format_rpcnode: mismatched count "
	   << keys.size () << " != " << rpcnode->count
	   << " at depth " << depth << " / " << prefix << "\n";
      // Lose extra keys if too many.
      while (keys.size () > 64)
        keys.pop_back ();
      rpcnode->count = keys.size ();
      if (!keys.size ()) {
	rpcnode->hash = 0;
      } else {
	sha1ctx sc;
	for (u_int i = 0; i < keys.size (); i++)
	  sc.update (keys[i].bytes, keys[i].size);
	sc.final (rpcnode->hash.bytes);
      }
    }

    rpcnode->child_hash.setsize (keys.size ());
    for (u_int i = 0; i < keys.size (); i++) {
      rpcnode->child_hash[i] = keys[i];
    }
  }
}

// Check whether [l1, r1] overlaps [l2, r2] on the circle.
static bool
overlap (const bigint &l1, const bigint &r1, const bigint &l2, const bigint &r2)
{
  // There might be a more efficient way to do this..
  return (betweenbothincl (l1, r1, l2) || betweenbothincl (l1, r1, r2)
	  || betweenbothincl (l2, r2, l1) || betweenbothincl (l2, r2, r1));
}

static void
compare_keylists (const vec<chordID> &lkeys,
		  const vec<chordID> &vrkeys,
		  chordID rngmin, chordID rngmax,
		  missingfnc_t missingfnc)
{
  // populate a hash table with the remote keys
  qhash<chordID, int, hashID> rkeys;
  for (u_int i = 0; i < vrkeys.size (); i++) {
    if (betweenbothincl (rngmin, rngmax, vrkeys[i])) {
      // trace << "remote key: " << vrkeys[i] << "\n";
      rkeys.insert (vrkeys[i], 1);
    }
  }

  // do I have something he doesn't have?
  for (unsigned int i = 0; i < lkeys.size (); i++) {
    if (!rkeys[lkeys[i]] &&
	betweenbothincl (rngmin, rngmax, lkeys[i])) {
      trace << "remote missing [" << rngmin << ", "
	    << rngmax << "] key=" << lkeys[i] << "\n";
      (*missingfnc) (lkeys[i], false);
    } else {
      if (rkeys[lkeys[i]]) trace << "remote has " << lkeys[i] << "\n";
      else trace << "out of range: " << lkeys[i] << "\n";
      rkeys.remove (lkeys[i]);
    }
  }

  //anything left: he has and I don't
  qhash_slot<chordID, int> *slot = rkeys.first ();
  while (slot) {
    trace << "local missing [" << rngmin << ", "
	  << rngmax << "] key=" << slot->key << "\n";
    (*missingfnc) (slot->key, true);
    slot = rkeys.next (slot);
  }

}
// }}}

// {{{ merkle_getkeyrange
// {{{ merkle_getkeyrange declarations
class merkle_getkeyrange {
private:
  uint vnode;
  dhash_ctype ctype;
  bigint rngmin;
  bigint rngmax;
  bigint current;
  missingfnc_t missing;
  rpcfnc_t rpcfnc;
  vec<chordID> lkeys;

  cbv completecb;

  void finish ();
  void doRPC (int procno, ptr<void> in, void *out, aclnt_cb cb);
  void go ();
  void getkeys_cb (ref<getkeys_arg> arg, ref<getkeys_res> res, clnt_stat err);

public:
  ~merkle_getkeyrange () {}
  merkle_getkeyrange (uint vnode, dhash_ctype ctype,
		      bigint rngmin, bigint rngmax,
		      const vec<chordID> &plkeys,
		      missingfnc_t missing, rpcfnc_t rpcfnc,
		      cbv completecb)
    : vnode (vnode), ctype (ctype), rngmin (rngmin),
      rngmax (rngmax), current (rngmin),
      missing (missing), rpcfnc (rpcfnc), lkeys (plkeys),
      completecb (completecb)
    { go (); }
};
// }}}
// {{{ merkle_getkeyrange utility
void
merkle_getkeyrange::finish ()
{
  delaycb (0, completecb);
  delete this;
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
// }}}
void
merkle_getkeyrange::go ()
{
  if (!betweenbothincl (rngmin, rngmax, current)) {
    trace << "merkle_getkeyrange::go () ==> DONE\n";
    finish ();
    return;
  }

  ref<getkeys_arg> arg = New refcounted<getkeys_arg> ();
  arg->ctype = ctype;
  arg->vnode = vnode;
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
    finish ();
    return;
  } else if (res->status != MERKLE_OK) {
    warn << "GETKEYS: protocol error " << res->status << "\n";
    finish ();
    return;
  }
//  warn << "getkeys_cb: [" << arg->rngmin << "," << arg->rngmax << "] "
//       << res->resok->keys.size () << " keys, with "
//       << (res->resok->morekeys ? "more" : "no") << " keys left.\n";

  // Assuming keys are sent back in increasing clockwise order
  vec<chordID> rkeys;
  for (u_int i = 0; i < res->resok->keys.size (); i++)
    rkeys.push_back (res->resok->keys[i]);

  chordID sentmax = rngmax;
  if (res->resok->morekeys && res->resok->keys.size () > 0)
    sentmax = res->resok->keys.back ();
  compare_keylists (lkeys, rkeys, current, sentmax, missing);

  current = incID (sentmax);
  if (!res->resok->morekeys) {
    finish ();
    return;
  }
  go ();
}
// }}}

// {{{ merkle_syncer
// {{{ merkle_syncer utility
merkle_syncer::merkle_syncer (uint vnode, dhash_ctype ctype,
			      ptr<merkle_tree> ltree,
			      rpcfnc_t rpcfnc, missingfnc_t missingfnc)
  : vnode (vnode), ctype (ctype), ltree (ltree), rpcfnc (rpcfnc),
    missingfnc (missingfnc), completecb (cbi_null),
    outstanding_sendnodes (0),
    outstanding_keyranges (0)
{
  deleted = New refcounted<bool> (false);
  fatal_err = NULL;
  sync_done = false;
}

merkle_syncer::~merkle_syncer ()
{
  *deleted = true;
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
  if (sync_done) {
    warning << "duplicate setdone for " << (u_int) this << "\n";
    return;
  }
  if (completecb != cbi_null) {
    // Return 0 if everything was ok, 1 otherwise.
    completecb (fatal_err.cstr () != NULL);
    completecb = cbi_null;
  }
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

  return sb;
}
// }}}

void
merkle_syncer::sync (bigint rngmin, bigint rngmax, cbi cb)
{
  assert (outstanding_sendnodes == 0);
  assert (outstanding_keyranges == 0);
  assert (st.size () == 0);
  sync_done = false;
  local_rngmin = rngmin;
  local_rngmax = rngmax;
  completecb = cb;

  // start at the root of the merkle tree
  sendnode (0, 0);
}

void
merkle_syncer::sendnode (u_int depth, const merkle_hash &prefix)
{
  ref<sendnode_arg> arg = New refcounted<sendnode_arg> ();
  ref<sendnode_res> res = New refcounted<sendnode_res> ();

  u_int lnode_depth;
  merkle_node *lnode = ltree->lookup (&lnode_depth, depth, prefix);
  // OK to assert this: since depth-1 is an index node, we know that
  //                    it created all of its depth children when
  //                    it split. --FED
  assert (lnode);
  assert (lnode_depth == depth);

  format_rpcnode (ltree, depth, prefix, lnode, &arg->node);
  arg->vnode = vnode;
  arg->ctype = ctype;
  arg->rngmin = local_rngmin;
  arg->rngmax = local_rngmax;
  ltree->lookup_release (lnode);
  outstanding_sendnodes++;
  doRPC (MERKLESYNC_SENDNODE, arg, res,
	 wrap (mkref (this), &merkle_syncer::sendnode_cb, deleted, arg, res));
}

void
merkle_syncer::sendnode_cb (ptr<bool> deleted,
			    ref<sendnode_arg> arg, ref<sendnode_res> res,
			    clnt_stat err)
{
  if (*deleted || sync_done)
    return;
  outstanding_sendnodes--;
  if (err) {
    error (strbuf () << "SENDNODE: rpc error " << err);
    return;
  } else if (res->status != MERKLE_OK) {
    warn << "SENDNODE: protocol error " << res->status << "\n";
  } else {
    merkle_rpc_node *rnode = &res->resok->node;
    merkle_node *lnode = ltree->lookup_exact (rnode->depth,
	rnode->prefix);
    if (lnode) {
      compare_nodes (local_rngmin, local_rngmax, lnode, rnode);
      ltree->lookup_release (lnode);
    } else {
      // If we no longer have a node at this address, it must mean
      // we used to but deletions have shrank our tree.
      // Let's just skip this subtree and get it the next time.
      warn << "lookup failed: " << rnode->prefix 
	   << " at " << rnode->depth << "\n";
    }
  }

  next ();
}

void
merkle_syncer::next (void)
{
  trace << "local range [" <<  local_rngmin << "," << local_rngmax << "]\n";
  assert (!sync_done);
  assert (!fatal_err);

  // st is queue of pending index nodes
  while (st.size ()) {
    pair<merkle_rpc_node, int> &p = st.back ();
    merkle_rpc_node *rnode = &p.first;
    assert (!rnode->isleaf);

    merkle_node *lnode = ltree->lookup_exact (rnode->depth, rnode->prefix);

    if (!lnode || lnode->isleaf ()) {
      // Inconsistencies are to be expected; skip this subtree for now.
      warnx << "lookup_exact didn't match for " << rnode->prefix << " at depth " << rnode->depth << "\n";
      if (lnode)
	ltree->lookup_release (lnode);
      st.pop_back ();
      continue;
    }

    trace << "starting from slot " << p.second << "\n";

    while (p.second < 64) {
      u_int i = p.second;
      p.second += 1;
      trace << "CHECKING: " << i << " of " << rnode->prefix << " at depth " << rnode->depth << "\n";

      bigint remote = static_cast<bigint> (rnode->child_hash[i]);
      bigint local = static_cast<bigint> (lnode->child_hash (i));

      u_int depth = rnode->depth + 1;

      //prefix is the high bits of the first key
      // the node is responsible for.
      merkle_hash prefix = rnode->prefix;
      prefix.write_slot (rnode->depth, i);
      bigint slot_rngmin = static_cast<bigint> (prefix);
      bigint slot_width = bigint (1) << (160 - 6*depth);
      bigint slot_rngmax = slot_rngmin + slot_width - 1;

      bool overlaps = overlap (local_rngmin, local_rngmax, slot_rngmin, slot_rngmax);

      strbuf tr;
      if (remote != local) {
	tr << " differ. local " << local << " != remote " << remote;

	if (overlaps) {
	  tr << " .. sending\n";
	  sendnode (depth, prefix);
	  trace << tr;
	  ltree->lookup_release (lnode);
	  return;
	} else {
	  tr << " .. not sending\n";
	}
      } else {
	tr << " same. local " << local << " == remote " << remote << "\n";
      }
      trace << tr;
    }

    ltree->lookup_release (lnode);
    assert (p.second == 64);
    st.pop_back ();
  }
  trace << "DONE with internal nodes in NEXT\n";

  if (!outstanding_keyranges && !outstanding_sendnodes) {
    setdone ();
    trace << "All OK!\n";
  }
}

void
merkle_syncer::compare_nodes (bigint rngmin, bigint rngmax,
	       merkle_node *lnode, merkle_rpc_node *rnode)
{
  trace << (lnode->isleaf ()  ? "L" : "I")
       << " vs "
       << (rnode->isleaf ? "L" : "I")
       << " at " << rnode->prefix << " depth " << rnode->depth << "\n";

  if (lnode->hash == rnode->hash) {
    trace << "Hashes identical!\n";
    return;
  }
  if (!lnode->isleaf () && !rnode->isleaf) {
    st.push_back (pair<merkle_rpc_node, int> (*rnode, 0));
    return;
  }

  vec<chordID> lkeys = ltree->database_get_IDs (rnode->depth,
						rnode->prefix);
  if (rnode->isleaf) {
    vec<chordID> rkeys;
    for (u_int i = 0; i < rnode->child_hash.size (); i++)
	rkeys.push_back (static_cast<bigint> (rnode->child_hash[i]));

    compare_keylists (lkeys, rkeys, rngmin, rngmax, missingfnc);
  } else if (lnode->isleaf () && !rnode->isleaf) {
    bigint tmpmin = static_cast<bigint> (rnode->prefix);
    bigint node_width = bigint (1) << (160 - 6*rnode->depth);
    bigint tmpmax = tmpmin + node_width - 1;

    assert (tmpmin < tmpmax);
    // further constrain to be within the host's range of interest
    // This can be purely inside, at the start, at the end,
    // or at the outsides.
    if (rngmin < rngmax) {
      // Completely contained, or only partial overlap
      if (between (tmpmin, tmpmax, rngmin))
	tmpmin = rngmin;
      if (between (tmpmin, tmpmax, rngmax))
	tmpmax = rngmax;
      outstanding_keyranges++;
      vNew merkle_getkeyrange (vnode, ctype, tmpmin, tmpmax, lkeys,
	  missingfnc, rpcfnc,
	  wrap (mkref (this), &merkle_syncer::collect_keyranges, deleted));
    } else {
      if (betweenbothincl (rngmin, maxID, tmpmin) &&
	  betweenbothincl (0, rngmax, tmpmax))
      {
	// node is completely one of our ends.
	outstanding_keyranges++;
	vNew merkle_getkeyrange (vnode, ctype, tmpmin, tmpmax, lkeys,
	    missingfnc, rpcfnc,
	    wrap (mkref (this), &merkle_syncer::collect_keyranges, deleted));
      } else {
	// partial overlap; check right and left hand sides.
	if (betweenbothincl (rngmin, maxID, tmpmax)) {
	  outstanding_keyranges++;
	  vNew merkle_getkeyrange (vnode, ctype, rngmin, tmpmax, lkeys,
	      missingfnc, rpcfnc,
	      wrap (mkref (this), &merkle_syncer::collect_keyranges, deleted));
	}
	if (betweenbothincl (0, rngmax, tmpmin)) {
	  outstanding_keyranges++;
	  vNew merkle_getkeyrange (vnode, ctype, tmpmin, rngmax, lkeys,
	      missingfnc, rpcfnc,
	      wrap (mkref (this), &merkle_syncer::collect_keyranges, deleted));
	}
      }
    }
  }
}

void
merkle_syncer::collect_keyranges (ptr<bool> deleted)
{
  if (*deleted || sync_done)
    return;
  assert (outstanding_keyranges > 0);
  outstanding_keyranges--;
  if (!outstanding_keyranges)
    next ();
}
// }}}

/* vim:set foldmethod=marker: */
