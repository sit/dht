#ifndef _MERKLE_TREE_H_
#define _MERKLE_TREE_H_

#include "async.h"
#include "sha1.h"
#include "merkle_hash.h"
#include "merkle_node.h"

class location;
class merkle_tree;
class block_status_manager;

struct merkle_tree_stats {
  uint32 nodes_per_level[merkle_hash::NUM_SLOTS];
  uint32 empty_leaves_per_level[merkle_hash::NUM_SLOTS];
  uint32 leaves_per_level[merkle_hash::NUM_SLOTS];
  uint32 internals_per_level[merkle_hash::NUM_SLOTS];

  uint32 num_nodes;
  uint32 num_leaves;
  uint32 num_empty_leaves;
  uint32 num_internals;
};


struct merkle_key {
  chordID id;
  itree_entry<merkle_key> ik;

  merkle_key (chordID id) : id (id) {};
  merkle_key (merkle_hash id) : id (tobigint(id)) {};
};

class merkle_tree {
protected:
  bool do_rehash;
  void _hash_tree (u_int depth, const merkle_hash &key, merkle_node *n, bool check);
  void rehash (u_int depth, const merkle_hash &key, merkle_node *n);
  void count_blocks (u_int depth, const merkle_hash &key,
		     array<u_int64_t, 64> &nblocks);
  void leaf2internal (u_int depth, const merkle_hash &key, merkle_node *n);

  itree<chordID, merkle_key, &merkle_key::id, &merkle_key::ik> keylist;
  virtual void remove (u_int depth, merkle_hash &key, merkle_node *n);
  virtual int insert (u_int depth, merkle_hash &key, merkle_node *n);
  virtual merkle_node *lookup (u_int *depth, u_int max_depth, 
		       const merkle_hash &key, merkle_node *n);

public:
  enum { max_depth = merkle_hash::NUM_SLOTS }; // XXX off by one? or two?
  merkle_node root; // public for testing only
  merkle_tree_stats stats;

  merkle_tree ();
  virtual ~merkle_tree ();
  void remove (merkle_hash &key);
  void remove (const chordID &id);
  void remove (const chordID &id, const u_int32_t aux);
  int insert (merkle_hash &key);
  int insert (const chordID &id);
  int insert (const chordID &id, const u_int32_t aux);
  merkle_node *lookup_exact (u_int depth, const merkle_hash &key);
  merkle_node *lookup (u_int depth, const merkle_hash &key);
  merkle_node *lookup (u_int *depth, u_int max_depth, const merkle_hash &key);
  merkle_node *lookup (const merkle_hash &key);
  void clear ();

  // If bulk-modifying the tree, it is undesirable to rehash tree after each
  // mod.  In that case, users should disable rehashing on modifications
  // until all modifications are complete, hash_tree, and then re-enable.
  void set_rehash_on_modification (bool enable);
  void hash_tree ();

  vec<merkle_hash> database_get_keys (u_int depth, const merkle_hash &prefix);
  vec<chordID> database_get_IDs (u_int depth, const merkle_hash &prefix);
  bool key_exists (chordID key) { return keylist[key] != NULL; };
  vec<chordID> get_keyrange (chordID min, chordID max, u_int n);

  void dump ();
  void check_invariants ();
  void compute_stats ();
  void stats_helper (uint depth, merkle_node *n);

};




#endif /* _MERKLE_TREE_H_ */
