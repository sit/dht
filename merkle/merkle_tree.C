#include "merkle_tree.h"

void
merkle_tree::rehash (u_int depth, const merkle_hash &key, merkle_node *n)
{
  if (!db)
    return;

  n->hash = 0;
  if (n->count == 0)
    return;
  
  ///warn << "rehash: depth " << depth << ", key " << key << "\n";
  
  sha1ctx sc;
  if (n->isleaf ()) {
    assert (n->count > 0 && n->count <= 64);
    merkle_hash prefix = key;
    prefix.clear_suffix (depth);
    ///warn << "prefix: " << prefix << "\n";

#ifdef NEWDB
    vec<merkle_hash> keys = database_get_keys (db, depth, prefix);
    for (u_int i = 0; i < keys.size (); i++)
      sc.update (keys[i].bytes, keys[i].size);
#else
    for (block *cur = db->cursor (prefix) ; cur; cur = db->next (cur)) {
      if (!prefix_match (depth, cur->key, key))
	break;
      sc.update (cur->key.bytes, cur->key.size);
      ///warn << "LEAF: update " << cur->key << "\n";
    }
#endif

  } else {
    for (int i = 0; i < 64; i++) {
      merkle_node *child = n->child (i); 
      sc.update (child->hash.bytes, child->hash.size);
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
  
  if (!db)
    return;

  // XXX duplicates code with rehash ()
  merkle_hash prefix = key;
  prefix.clear_suffix (depth);
  
#ifdef NEWDB
  vec<merkle_hash> keys = database_get_keys (db, depth, prefix);
  for (u_int i = 0; i < keys.size (); i++) {
    u_int32_t branch = keys[i].read_slot (depth);
    nblocks[branch] += 1;
  }
#else
  for (block *cur = db->cursor (prefix); cur; cur = db->next (cur)) {
    if (!prefix_match (depth, cur->key, key))
      break;
    u_int32_t branch = cur->key.read_slot (depth);
    nblocks[branch] += 1;
  }
#endif

}


void
merkle_tree::leaf2internal (u_int depth, const merkle_hash &key, merkle_node *n) 
{
  ///warn << "leaf2internal >>>>>>>>>>>>>>>>>>>>>\n";
  assert (n->isleaf ());
  array<u_int64_t, 64> nblocks;
  count_blocks (depth, key, nblocks);
  n->leaf2internal ();
  
  merkle_hash prefix = key;
  prefix.clear_suffix (depth);
  
  u_int xmax = (depth == merkle_hash::NUM_SLOTS) ? 16 : 64;
  for (u_int i = 0; i < xmax; i++) {
    ///warn << "leaf2internal [" << i << "] = " << nblocks[i] << "\n";
    
    merkle_node *child = n->child (i);
    child->initialize (nblocks[i]);
    prefix.write_slot (depth, i);
    rehash (depth+1, prefix, child);
  }
  ///warn << "leaf2internal <<<<<<<<<<<<<<<<<<<<<<\n";
}


void
merkle_tree::remove (u_int depth, block *b, merkle_node *n)
{
  if (n->isleaf ()) {
    if (db) {
#ifdef NEWDB      
      database_remove (db, b);
      assert (!database_lookup (db, b->key));
#else
      db->remove (b);
      assert (!db->lookup (b->key));
#endif

    }
  } else {
    u_int32_t branch = b->key.read_slot (depth);
    remove (depth+1, b, n->child (branch));
  }
  
  assert (n->count != 0);
  n->count -= 1;
  if (!n->isleaf () && n->count <= 64)
    n->internal2leaf ();
  rehash (depth, b->key, n);
}


void
merkle_tree::insert (u_int depth, block *b, merkle_node *n)
{
  ///warn << "insert, b->key " << b->key << " >>>>>>>>>>>>>>>>>>>>\n";  
  
  if (n->isleaf () && n->leaf_is_full ())
    leaf2internal (depth, b->key, n);
  
  if (n->isleaf ()) {
#ifdef NEWDB 
    database_insert (db, b);
#else
    if (db)
      db->insert (b);
#endif
  } else {
    u_int32_t branch = b->key.read_slot (depth);
    ///warn << "depth " << depth << ", branch " << branch << "\n";
    insert (depth+1, b, n->child (branch));
  }
  
  n->count += 1;
  rehash (depth, b->key, n);
  ///warn << "IH: " << n->hash << "\n";
  ///warn << "insert, b->key " << b->key << " <<<<<<<<<<<<<<<<<<<<\n";  
}

merkle_node *
merkle_tree::lookup (u_int *depth, u_int max_depth, merkle_hash &key, merkle_node *n)
{
  // recurse down as much as possible
  if (*depth == max_depth || n->isleaf ())
    return n;
  u_int32_t branch = key.read_slot (*depth);
  *depth += 1;
  return lookup (depth, max_depth, key, n->child (branch));
}

merkle_tree::merkle_tree (dbfe *db) 
  : db (db)
{
  // assert db is initially empty 

#ifdef NEWDB
  vec<merkle_hash> keys = database_get_keys (db, 0, merkle_hash (0));
  assert (!db || (keys.size () == 0));
#else
  assert (!db ||!db->first ());
#endif
}

void
merkle_tree::remove (block *b)
{
  // assert block must exist..
#ifdef NEWDB
  assert (db);
  if (!database_lookup (db, b->key)) {
    warn << "merkle_tree::remove: key does not exists " << b->key << "\n";
    return;
  }
  assert (!db || database_lookup (db, b->key));
#else
  assert (!db || db->lookup (b->key));
#endif

  remove (0, b, &root);
}

void
merkle_tree::insert (block *b)
{
  //warn <<  "\n\n\n **** merkle_tree::insert: " << b->key << "\n";

  // forbid dups..
#ifdef NEWDB
  assert (db);
  if (database_lookup (db, b->key)) {
    warn << "merkle_tree::insert: key already exists " << b->key << "\n";
    return;
  }
#else
  assert (!db || !db->lookup (b->key));
#endif
  ///dump ();
  //check_invariants ();
  insert (0, b, &root);
  ///dump ();
  //check_invariants ();
}


// return the node a given depth matching key
// returns NULL if no such node exists
merkle_node *
merkle_tree::lookup_exact (u_int depth, merkle_hash &key)
{
  u_int realdepth = 0;
  merkle_node *n = lookup (&realdepth, depth, key, &root);
  assert (realdepth <= depth);
  return  (realdepth != depth) ? NULL : n;
}

// Might return parent.
// Never returns NULL.
// Never returns a node deeper than depth.
merkle_node *
merkle_tree::lookup (u_int depth, merkle_hash &key)
{
  u_int depth_ignore = 0;
  return lookup (&depth_ignore, depth, key, &root);
}

merkle_node *
merkle_tree::lookup (u_int *depth, u_int max_depth, merkle_hash &key)
{
  *depth = 0;
  return lookup (depth, max_depth, key, &root);
}

// return the deepest node whose prefix matches key
// Never returns NULL
merkle_node *
merkle_tree::lookup (merkle_hash &key)
{
  return lookup (merkle_hash::NUM_SLOTS, key);
}

void
merkle_tree::dump ()
{
  root.dump (0);
}

void
merkle_tree::check_invariants ()
{
  root.check_invariants (0, 0, db);
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
  stats_helper (0, &root);
  
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

  uint64 mn = max_depth;
  uint64 mx = 0;
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

  warn << "blocks: " << root.count << "\n";
  snprintf (buf, sizeof (buf), 
	    "blocks/node: %f\n"
	    "blocks/leaf: %f\n"
	    "blocks/non-empty-leaf: %f\n"
	    "blocks/internal: %f\n",
	    root.count / (float)stats.num_nodes,
	    root.count / (float)stats.num_leaves,
	    root.count / (float)(stats.num_leaves - stats.num_empty_leaves),
	    root.count / (float)stats.num_internals);
  warn << buf;
}








