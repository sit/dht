#ifndef _MERKLE_TREE_H_
#define _MERKLE_TREE_H_

#include "async.h"
#include "sha1.h"
#include "merkle_hash.h"
#include "merkle_node.h"

class merkle_tree;

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


class merkle_tree {
private:
  void rehash (u_int depth, const merkle_hash &key, merkle_node *n);
  void count_blocks (u_int depth, const merkle_hash &key,
		     array<u_int64_t, 64> &nblocks);
  void leaf2internal (u_int depth, const merkle_hash &key, merkle_node *n);
  void remove (u_int depth, block *b, merkle_node *n);
  void insert (u_int depth, block *b, merkle_node *n);
  merkle_node *lookup (u_int *depth, u_int max_depth, const merkle_hash &key, merkle_node *n);

public:
  enum { max_depth = merkle_hash::NUM_SLOTS }; // XXX off by one? or two?
  dbfe *db;     // public for testing only
  merkle_node root; // ditto
  merkle_tree_stats stats;


  merkle_tree (dbfe *db); 
  void remove (block *b);
  void insert (block *b);
  merkle_node *lookup_exact (u_int depth, const merkle_hash &key);
  merkle_node *lookup (u_int depth, const merkle_hash &key);
  merkle_node *lookup (u_int *depth, u_int max_depth, const merkle_hash &key);
  merkle_node *lookup (const merkle_hash &key);
  void clear ();

  void dump ();
  void check_invariants ();
  void compute_stats ();
  void stats_helper (uint depth, merkle_node *n);

};




#endif /* _MERKLE_TREE_H_ */
