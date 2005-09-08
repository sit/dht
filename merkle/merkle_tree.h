#ifndef _MERKLE_TREE_H_
#define _MERKLE_TREE_H_

#include "async.h"
#include "sha1.h"
#include "merkle_hash.h"
#include "merkle_node.h"
#include "qhash.h"
#include "libadb.h"
#include "skiplist.h"

class block_status_manager;
class location;
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


struct merkle_key {
  chordID id;
  sklist_entry<merkle_key> sk;

  merkle_key (chordID id) : id (id) {};
  merkle_key (merkle_hash id) : id (tobigint(id)) {};
};

class merkle_tree {
private:
  void rehash (u_int depth, const merkle_hash &key, merkle_node *n);
  void count_blocks (u_int depth, const merkle_hash &key,
		     array<u_int64_t, 64> &nblocks);
  void leaf2internal (u_int depth, const merkle_hash &key, merkle_node *n);
  void remove (u_int depth, merkle_hash &key, merkle_node *n);
  int insert (u_int depth, merkle_hash &key, merkle_node *n);
  merkle_node *lookup (u_int *depth, u_int max_depth, 
		       const merkle_hash &key, merkle_node *n);

  void iterate_cb (ptr<adb> realdb, adb_status stat, vec<chordID> keys);
  void iterate_custom_cb (ptr<block_status_manager> bsm, 
			  chordID localID,
			  chordID remoteID,
			  ptr<adb> realdb, 
			  cbv cb,
			  adb_status stat, 
			  vec<chordID> keys);

  skiplist<merkle_key, chordID, &merkle_key::id, &merkle_key::sk> sk_keys;
public:
  enum { max_depth = merkle_hash::NUM_SLOTS }; // XXX off by one? or two?
  ptr<adb> db;     // public for testing only
  merkle_node root; // ditto
  merkle_tree_stats stats;

  merkle_tree (ptr<adb> db, bool populate); 
  merkle_tree (ptr<adb> realdb,
	       ptr<block_status_manager> bsm,
	       chordID remoteID,
	       vec<ptr<location> > succs,
	       dhash_ctype ctype,
	       cbv cb);
  ~merkle_tree ();
  void remove (merkle_hash &key);
  int insert (merkle_hash &key);
  merkle_node *lookup_exact (u_int depth, const merkle_hash &key);
  merkle_node *lookup (u_int depth, const merkle_hash &key);
  merkle_node *lookup (u_int *depth, u_int max_depth, const merkle_hash &key);
  merkle_node *lookup (const merkle_hash &key);
  void clear ();


  vec<merkle_hash> database_get_keys (u_int depth, const merkle_hash &prefix);
  vec<chordID> database_get_IDs (u_int depth, const merkle_hash &prefix);
  bool key_exists (chordID key) { return sk_keys.search (key) != NULL; };
  vec<chordID> get_keyrange (chordID min, chordID max, u_int n);

  void dump ();
  void check_invariants ();
  void compute_stats ();
  void stats_helper (uint depth, merkle_node *n);

};




#endif /* _MERKLE_TREE_H_ */
