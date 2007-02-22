#include <chord_types.h>
#include <id_utils.h>
#include "merkle_tree.h"
#include "dhash_common.h"

static merkle_key *
closestsucc (itree<chordID, merkle_key, &merkle_key::id, &merkle_key::ik> &keylist, chordID k)
{
  merkle_key *p = NULL;
  merkle_key *n = keylist.root ();
  // warnx << "closestsucc k = " << k << "\n";
  if (!n)
    return NULL;
  while (n) {
    p = n;
    // warnx << "closestsucc n->id = " << n->id << "\n";
    if (k < n->id)
      n = keylist.left (n);
    else if (k > n->id)
      n = keylist.right (n);
    else
      return n;
  }
  if (k < p->id)
    return p;
  else {
    // Wrap around at the end.
    n = keylist.next (p);
    return n ? n : keylist.first ();
  }
}

merkle_tree::merkle_tree () :
  do_rehash (true)
{
  root = New merkle_node();
  // warn << "root: " << root->isleaf() << "\n";
}

merkle_tree::~merkle_tree ()
{
  keylist.deleteall_correct ();
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


void
merkle_tree::count_blocks (u_int depth, const merkle_hash &key,
			   array<u_int64_t, 64> &nblocks)
{
  for (int i = 0; i < 64; i++)
    nblocks[i] = 0;
  
  // XXX duplicates code with rehash ()
  merkle_hash prefix = key;
  prefix.clear_suffix (depth);
  
  vec<merkle_hash> keys = database_get_keys (depth, prefix);
  for (u_int i = 0; i < keys.size (); i++) {
    u_int32_t branch = keys[i].read_slot (depth);
    nblocks[branch] += 1;
  }

}


void
merkle_tree::leaf2internal (u_int depth, const merkle_hash &key, 
			    merkle_node *n) 
{
  assert (n->isleaf ());
  array<u_int64_t, 64> nblocks;
  count_blocks (depth, key, nblocks);
  n->leaf2internal ();
  
  merkle_hash prefix = key;
  prefix.clear_suffix (depth);
  
  u_int xmax = (depth == merkle_hash::NUM_SLOTS) ? 16 : 64;
  for (u_int i = 0; i < xmax; i++) {
    // warn << "leaf2internal [" << i << "] = " << nblocks[i] << "\n";
    
    merkle_node *child = n->child (i);
    child->initialize (nblocks[i]);
    prefix.write_slot (depth, i);
    rehash (depth+1, prefix, child);
  }
}


void
merkle_tree::remove (u_int depth, merkle_hash& key, merkle_node *n)
{
  if (n->isleaf ()) {
    chordID k = tobigint (key);
    merkle_key *mkey = keylist[k];
    assert (mkey);
    keylist.remove (mkey);
  } else {
    u_int32_t branch = key.read_slot (depth);
    remove (depth+1, key, n->child (branch));
  }
  
  assert (n->count != 0);
  n->count -= 1;
  if (!n->isleaf () && n->count <= 64)
    n->internal2leaf ();
  rehash (depth, key, n);
}


int
merkle_tree::insert (u_int depth, merkle_hash& key, merkle_node *n)
{
  int ret = 0;
    
  if (n->isleaf () && n->leaf_is_full ())
    leaf2internal (depth, key, n);
  
  if (n->isleaf ()) {
    merkle_key *k = New merkle_key (key);
    assert (!keylist[k->id]);
    keylist.insert (k);
  } else {
    u_int32_t branch = key.read_slot (depth);
    ret = insert (depth+1, key, n->child (branch));
  }
  
  n->count += 1;
  rehash (depth, key, n);
  return ret;
}

int
merkle_tree::insert (merkle_hash &key)
{

  if (keylist[tobigint (key)])
    fatal << "merkle_tree::insert: key already exists " << key << "\n";

  return insert (0, key, get_root());
}

int
merkle_tree::insert (const chordID &id)
{
  merkle_hash mkey = to_merkle_hash (id);
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
  merkle_hash mkey = to_merkle_hash (key);
  return insert (mkey);
}

void
merkle_tree::remove (merkle_hash &key)
{
  // assert block must exist..
  str foo;
  if (!keylist[tobigint (key)])
    fatal << (u_int) this << " merkle_tree::remove: key does not exist " << key << "\n";

  remove (0, key, get_root());
}

void
merkle_tree::remove (const chordID &id)
{
  merkle_hash mkey = to_merkle_hash (id);
  remove (mkey);
}

void
merkle_tree::remove (const chordID &id, const u_int32_t aux)
{
  chordID key = id;
  key >>= 32;
  key <<= 32;
  assert (key > 0);
  key |= aux;
  merkle_hash mkey = to_merkle_hash (key);
  remove (mkey);
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

// return the node a given depth matching key
// returns NULL if no such node exists
merkle_node *
merkle_tree::lookup_exact (u_int depth, const merkle_hash &key)
{
  u_int realdepth = 0;
  merkle_node *n = lookup (&realdepth, depth, key, get_root());
  assert (realdepth <= depth);
  return  (realdepth != depth) ? NULL : n;
}

// Might return parent.
// Never returns NULL.
// Never returns a node deeper than depth.
merkle_node *
merkle_tree::lookup (u_int depth, const merkle_hash &key)
{
  u_int depth_ignore = 0;
  return lookup (&depth_ignore, depth, key, get_root());
}

merkle_node *
merkle_tree::lookup (u_int *depth, u_int max_depth, const merkle_hash &key)
{
  *depth = 0;
  return lookup (depth, max_depth, key, get_root());
}

// return the deepest node whose prefix matches key
// Never returns NULL
merkle_node *
merkle_tree::lookup (const merkle_hash &key)
{
  return lookup (merkle_hash::NUM_SLOTS, key);
}

vec<merkle_hash>
merkle_tree::database_get_keys (u_int depth, const merkle_hash &prefix)
{
  vec<merkle_hash> ret;
  merkle_key *cur = closestsucc (keylist, tobigint (prefix));
  
  while (cur) {
    merkle_hash key = to_merkle_hash (cur->id);
    if (!prefix_match (depth, key, prefix))
      break;
    ret.push_back (key);
    cur = keylist.next (cur);
  } 
  return ret;
}

vec<chordID>
merkle_tree::database_get_IDs (u_int depth, const merkle_hash &prefix)
{
  vec<merkle_hash> mhash = database_get_keys (depth, prefix);
  vec<chordID> ret;
  for (unsigned int i = 0; i < mhash.size (); i++)
    ret.push_back (tobigint(mhash[i]));
  return ret;
}

vec<chordID> 
merkle_tree::get_keyrange (chordID min, chordID max, u_int n)
{
  vec<chordID> keys;
  merkle_key *k = closestsucc (keylist, min);
  for (u_int i = 0; k && i < n; i++) {
    if (!betweenbothincl (min, max, k->id))
      break;
    keys.push_back (k->id);
    k = keylist.next (k);
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
  uint cutoff = max_depth - 1;
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

  u_int64_t mn = max_depth;
  u_int64_t mx = 0;
  double ave = 0;
  for (uint i = 0; i < max_depth; i++) {
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
