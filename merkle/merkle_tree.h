#ifndef _MERKLE_TREE_H_
#define _MERKLE_TREE_H_

#include <itree.h>
#include "merkle_hash.h"

struct merkle_tree_stats {
  u_int32_t nodes_per_level[merkle_hash::NUM_SLOTS];
  u_int32_t empty_leaves_per_level[merkle_hash::NUM_SLOTS];
  u_int32_t leaves_per_level[merkle_hash::NUM_SLOTS];
  u_int32_t internals_per_level[merkle_hash::NUM_SLOTS];

  u_int32_t num_nodes;
  u_int32_t num_leaves;
  u_int32_t num_empty_leaves;
  u_int32_t num_internals;
};

class merkle_node {
public:
  u_int32_t count;
  merkle_hash hash;

  virtual merkle_hash child_hash (u_int i) = 0;
  virtual merkle_node *child (u_int i) = 0;
  virtual bool isleaf () const = 0;
  virtual bool leaf_is_full () const {
    // XXX what about at the bottom level (count == 16)!!!!
    assert (isleaf ());
    return (count == 64);
  }
  virtual void internal2leaf () = 0;
  virtual void leaf2internal () = 0;
  virtual void dump (u_int depth) = 0;

  merkle_node () : count (0), hash (0) {}
  virtual ~merkle_node ();
};

struct merkle_key {
  chordID id;
  itree_entry<merkle_key> ik;

  merkle_key (chordID id) : id (id) {};
  merkle_key (merkle_hash id) : id (static_cast<bigint> (id)) {};
};

class merkle_tree {
protected:
  bool do_rehash;

  void _hash_tree (u_int depth, const merkle_hash &key, merkle_node *n, bool check);
  void rehash (u_int depth, const merkle_hash &key, merkle_node *n);
  void stats_helper (uint depth, merkle_node *n);

  merkle_node *lookup (u_int *depth, u_int max_depth,
		       const merkle_hash &key, merkle_node *n);

  // Internal functions that sub-classes must implement.
  virtual int remove (u_int depth, merkle_hash &key, merkle_node *n) = 0;
  virtual int insert (u_int depth, merkle_hash &key, merkle_node *n) = 0;

public:
  enum { MAX_DEPTH = merkle_hash::NUM_SLOTS }; // XXX off by one? or two?
  merkle_tree_stats stats;

  merkle_tree ();
  virtual ~merkle_tree ();

  // Sub-classes must implement the following methods
  virtual merkle_node *get_root () = 0;
  virtual int insert (merkle_hash &key) = 0;
  virtual int remove (merkle_hash &key) = 0;
  virtual bool key_exists (chordID key) = 0;
  virtual vec<merkle_hash> database_get_keys (u_int depth,
      const merkle_hash &prefix) = 0;
  virtual void get_keyrange_nowrap (const chordID &min,
      const chordID &max, u_int n, vec<chordID> &keys) = 0;
  virtual vec<chordID> get_keyrange (chordID min, chordID max, u_int n);

  // return the node a given depth matching key
  // returns NULL if no such node exists
  virtual merkle_node *lookup_exact (u_int depth, const merkle_hash &key) = 0;
  // Might return parent.
  // Never returns NULL.
  // Never returns a node deeper than depth.
  virtual merkle_node *lookup (u_int depth, const merkle_hash &key) = 0;

  // Return deepest node no deeper than max_depth matching key.
  // Return the depth of the node in *depth.
  virtual merkle_node *lookup (u_int *depth, u_int max_depth,
			       const merkle_hash &key) = 0;

  // Sub-classes may override the following methods
  virtual void lookup_release (merkle_node *n);
  virtual void sync (bool reopen = true);

  virtual vec<chordID> database_get_IDs (u_int depth, const merkle_hash &prefix);

  virtual void check_invariants ();

  // Sub-classes should not override the following methods
  int insert (const chordID &id);
  int insert (const chordID &id, const u_int32_t aux);
  int remove (const chordID &id);
  int remove (const chordID &id, const u_int32_t aux);
  merkle_node *lookup (const merkle_hash &key);

  bool key_exists (chordID key, uint aux);

  // If bulk-modifying the tree, it is undesirable to rehash tree
  // after each mod.  In that case, users should disable rehashing on
  // modifications until all modifications are complete, hash_tree,
  // and then re-enable.
  void set_rehash_on_modification (bool enable);
  void hash_tree ();

  void dump ();
  void compute_stats ();
};

class merkle_node_mem;

class merkle_tree_mem : public merkle_tree {
protected:
  merkle_node_mem *root;
  itree<chordID, merkle_key, &merkle_key::id, &merkle_key::ik> keylist;

  void count_blocks (u_int depth, const merkle_hash &key,
		     array<u_int64_t, 64> &nblocks);
  void leaf2internal (u_int depth, const merkle_hash &key, merkle_node *n);

  virtual int remove (u_int depth, merkle_hash &key, merkle_node *n);
  virtual int insert (u_int depth, merkle_hash &key, merkle_node *n);

public:
  merkle_tree_mem ();
  virtual ~merkle_tree_mem ();

  virtual merkle_node *get_root ();

  virtual int insert (merkle_hash &key);
  virtual int remove (merkle_hash &key);
  virtual bool key_exists (chordID key) {
    return keylist[key] != NULL;
  }
  virtual vec<merkle_hash> database_get_keys (u_int depth,
      const merkle_hash &prefix);
  void get_keyrange_nowrap (const chordID &min,
      const chordID &max, u_int n, vec<chordID> &keys);

  virtual merkle_node *lookup_exact (u_int depth, const merkle_hash &key);
  virtual merkle_node *lookup (u_int depth, const merkle_hash &key);
  virtual merkle_node *lookup (u_int *depth, u_int max_depth,
			       const merkle_hash &key);
};

#endif /* _MERKLE_TREE_H_ */
