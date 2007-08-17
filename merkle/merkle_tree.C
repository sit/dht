#include <chord_types.h>
#include <id_utils.h>
#include "merkle_tree.h"

merkle_node::~merkle_node ()
{
}

merkle_tree::merkle_tree () :
  do_rehash (true)
{
}
merkle_tree::~merkle_tree ()
{
}

void
merkle_tree::_hash_tree (u_int depth, const merkle_hash &prefix,
			 merkle_node *n, bool check = false)
{
  // Perform a post-order traversal of the entire tree where
  // at each node the operation is to recalculate the node's SHA1 hash
  // based on its children.  Children must be recalculated first.
  sha1ctx sc;
  u_int64_t ncount (0);
  if (!n->isleaf ()) {
    for (int i = 0; i < 64; i++) {
      merkle_node *child = n->child (i);
      ncount += child->count;
      merkle_hash nprefix (prefix);
      nprefix.write_slot (depth, i);
      _hash_tree (depth + 1, nprefix, child);
      sc.update (child->hash.bytes, child->hash.size);
    }
  } else {
    vec<merkle_hash> keys = database_get_keys (depth, prefix);
    ncount = keys.size ();
    assert (n->count <= 64);
    for (u_int i = 0; i < keys.size (); i++) {
      sc.update (keys[i].bytes, keys[i].size);
    }
  }
  if (check && ncount != n->count) {
    n->dump (depth);
    fatal << "merkle_tree: counted " << ncount
          << " children but had recorded " << n->count << " at "
          << (n->isleaf () ? "leaf" : "non-leaf")
	  << " at depth " << depth << " and prefix "
          << prefix << "\n";
  }

  merkle_hash nhash;
  if (n->count)
    sc.final (nhash.bytes);
  if (check && nhash != n->hash) {
    warn << "nhash   = " << nhash << "\n";
    warn << "n->hash = " << n->hash << "\n";
    warn << "n->count= " << n->count << "\n";
    fatal << "nhash of "
	  << (n->isleaf () ? "leaf" : "non-leaf")
	  << " didn't match at depth " << depth << " and prefix "
	  << prefix << "\n";
  }

  n->hash = nhash;
}

void
merkle_tree::hash_tree ()
{
  merkle_hash prefix (0);
  merkle_node *root = get_root ();
  _hash_tree (0, prefix, root);
  lookup_release (root);
}

void
merkle_tree::check_invariants ()
{
  // Invariants won't be true if not hashing.
  if (!do_rehash)
    return;
  merkle_hash prefix (0);
  merkle_node *root = get_root ();
  _hash_tree (0, prefix, root, true);
  lookup_release (root);  // semantic mismatch, I know, I know
}

void
merkle_tree::set_rehash_on_modification (bool enable)
{
  do_rehash = enable;
}

void
merkle_tree::rehash (u_int depth, const merkle_hash &key, merkle_node *n)
{
  if (!do_rehash)
    return;
  n->hash = 0;
  if (n->count == 0)
    return;
  
  sha1ctx sc;
  if (n->isleaf ()) {
    assert (n->count > 0 && n->count <= 64);
    merkle_hash prefix = key;
    prefix.clear_suffix (depth);
    vec<merkle_hash> keys = database_get_keys (depth, prefix);
    for (u_int i = 0; i < keys.size (); i++)
      sc.update (keys[i].bytes, keys[i].size);
  } else {
    for (int i = 0; i < 64; i++) {
      merkle_hash child = n->child_hash(i); 
      sc.update (child.bytes, child.size);
      ///warn << "INTE: update " << child->hash << "\n";
    }
  }
  sc.final (n->hash.bytes);
  ///warn << "final: " << n->hash << "\n";
}

int
merkle_tree::insert (const chordID &id)
{
  merkle_hash mkey (id);
  return insert (mkey);
}

int
merkle_tree::insert (const chordID &id, const u_int32_t aux)
{
  // When there is auxiliary information, we use the low-order bytes
  // of the key to hold the information.  This is used mostly for mutable
  // data that needs to distinguish whether or not the remote node has
  // the same version as the local node.
  chordID key = id;
  key >>= 32;
  key <<= 32;
  assert (key > 0);
  key |= aux;
  merkle_hash mkey (key);
  return insert (mkey);
}

int
merkle_tree::remove (const chordID &id)
{
  merkle_hash mkey (id);
  return remove (mkey);
}

int
merkle_tree::remove (const chordID &id, const u_int32_t aux)
{
  chordID key = id;
  key >>= 32;
  key <<= 32;
  assert (key > 0);
  key |= aux;
  merkle_hash mkey (key);
  return remove (mkey);
}

merkle_node *
merkle_tree::lookup (u_int *depth, u_int max_depth, 
		     const merkle_hash &key, merkle_node *n)
{
  // recurse down as much as possible
  if (*depth == max_depth || n->isleaf ())
    return n;
  u_int32_t branch = key.read_slot (*depth); 
  //the [6*depth, 6*(depth +1) bits determine which branch to follow
  // for a given key
  *depth += 1;
  return lookup (depth, max_depth, key, n->child (branch));
}

// return the deepest node whose prefix matches key
// Never returns NULL
merkle_node *
merkle_tree::lookup (const merkle_hash &key)
{
  return lookup (merkle_hash::NUM_SLOTS, key);
}

void
merkle_tree::lookup_release (merkle_node *n)
{
}

vec<chordID>
merkle_tree::database_get_IDs (u_int depth, const merkle_hash &prefix)
{
  vec<merkle_hash> mhash = database_get_keys (depth, prefix);
  vec<chordID> ret;
  for (unsigned int i = 0; i < mhash.size (); i++)
    ret.push_back (static_cast<bigint> (mhash[i]));
  return ret;
}

vec<chordID>
merkle_tree::get_keyrange (chordID min, chordID max, u_int n)
{
  vec<chordID> keys;
  if (min < max) {
    get_keyrange_nowrap (min, max, n, keys);
  } else {
    get_keyrange_nowrap (min, maxID, n, keys);
    get_keyrange_nowrap (0, max, n, keys);
  }
  return keys;
}


bool
merkle_tree::key_exists (chordID id, uint aux)
{
  chordID key = id;
  key >>= 32;
  key <<= 32;
  assert (key > 0);
  key |= aux;
  return key_exists (key);
}

void
merkle_tree::dump ()
{
  get_root()->dump (0);
}

void
merkle_tree::stats_helper (uint depth, merkle_node *n)
{
  stats.nodes_per_level[depth]++;
  stats.num_nodes++;

  if (n->isleaf ()) {
    if (n->count == 0) {
      stats.empty_leaves_per_level[depth]++;
      stats.num_empty_leaves++;
    }
    stats.leaves_per_level[depth]++;
    stats.num_leaves++;
  } else {
    stats.internals_per_level[depth]++;
    stats.num_internals++;
  }

  if (! n->isleaf ()) {
    for (uint i = 0; i < 64; i++) {
      merkle_node *child = n->child (i); 
      stats_helper (depth+1, child);
    }
  }
}


void
merkle_tree::compute_stats ()
{
  bzero (&stats, sizeof (stats));
  stats_helper (0, get_root());
  
  warn.fmt ("      %10s %10s %10s %10s\n", "leaves", "MT leaves", "internals", "nodes");

  // dont print the trailing zeroes...
  uint cutoff = MAX_DEPTH - 1;
  while (cutoff && !stats.nodes_per_level[cutoff])
    cutoff--;
  for (uint i = 0; i <= cutoff; i++) {
    warn.fmt ("%4d: %10d %10d %10d %10d\n",
	      i,
	      stats.leaves_per_level[i],
	      stats.empty_leaves_per_level[i],
	      stats.internals_per_level[i],
	      stats.nodes_per_level[i]);
  }
  warn << "-----------------------------------------\n";
  warn.fmt ("total %10d %10d %10d %10d\n",
	    stats.num_leaves, stats.num_empty_leaves, stats.num_internals, stats.num_nodes);
  assert (stats.num_leaves > 0);

  u_int64_t mn = MAX_DEPTH;
  u_int64_t mx = 0;
  double ave = 0;
  for (uint i = 0; i < MAX_DEPTH; i++) {
    if (stats.leaves_per_level[i] == 0)
      continue;

    ave += i * stats.leaves_per_level[i];
    mn = i < mn ? i : mn;
    mx = i > mx ? i : mx;
  }
  ave /= stats.num_leaves;

  warn << "depth min " << mn << "\n";
  warn << "depth max " << mx << "\n";
  char buf[2000];
  snprintf (buf, sizeof (buf), "depth ave %f\n", ave);
  warn << buf;

  warn << "blocks: " << get_root()->count << "\n";
  snprintf (buf, sizeof (buf), 
	    "blocks/node: %f\n"
	    "blocks/leaf: %f\n"
	    "blocks/non-empty-leaf: %f\n"
	    "blocks/internal: %f\n",
	    get_root()->count / (float)stats.num_nodes,
	    get_root()->count / (float)stats.num_leaves,
	    get_root()->count / (float)(stats.num_leaves - 
					stats.num_empty_leaves),
	    get_root()->count / (float)stats.num_internals);
  warn << buf;
}

void
merkle_tree::sync (bool reopen)
{
  return;
}
