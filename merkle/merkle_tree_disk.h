#ifndef _MERKLE_TREE_DISK_H_
#define _MERKLE_TREE_DISK_H_

#include "async.h"
#include "merkle_tree.h"

enum MERKLE_DISK_TYPE {
  MERKLE_DISK_INTERNAL = 0,
  MERKLE_DISK_LEAF = 1
};

struct merkle_char_key {
  char key[20];
};

struct merkle_index_metadata {
  u_int32_t root;
  u_int32_t num_leaf_free;
  u_int32_t num_internal_free;
  u_int32_t next_leaf;
  u_int32_t next_internal;
};

struct merkle_internal_node {
  merkle_char_key hashes[64];
  u_int32_t child_pointers[64];
  u_int32_t key_count;
};

struct merkle_leaf_node {
  merkle_char_key keys[64];
  u_int32_t key_count;
};

struct merkle_hash_id {
  merkle_hash hash;
  chordID id;
};

class merkle_node_disk : public merkle_node {
 private:
  array<merkle_hash_id, 64> *hashes;
  array<u_int32_t, 64> *children;

  FILE *_internal;
  FILE *_leaf;
  MERKLE_DISK_TYPE _type;
  u_int32_t _block_no;
  vec<merkle_node_disk *> to_delete;

 public:
  itree<chordID, merkle_key, &merkle_key::id, &merkle_key::ik> keylist;

  merkle_hash child_hash (u_int i);
  u_int32_t child_ptr (u_int i);
  merkle_node *child (u_int i);
  void leaf2internal ();
  void internal2leaf ();
  bool isleaf () const;
  void add_key (chordID key);
  void add_key (merkle_hash key);
  void set_child (merkle_node_disk *n, u_int i);
  void write_out ();
  void set_block_no (u_int32_t block_no) { _block_no = block_no; }
  u_int32_t get_block_no () { return _block_no; }
  void rehash ();
  void dump (u_int depth);

  merkle_node_disk (FILE *internal, FILE *leaf,
		    MERKLE_DISK_TYPE type, u_int32_t block_no);
  ~merkle_node_disk ();
};

class merkle_tree_disk : public merkle_tree {
private:
  str _index_name;
  str _internal_name;
  str _leaf_name;

  merkle_index_metadata _md;
  vec<u_int32_t> _free_leafs;
  vec<u_int32_t> _free_internals;
  vec<u_int32_t> _future_free_leafs;
  vec<u_int32_t> _future_free_internals;

  FILE *_index;
  FILE *_internal;
  FILE *_leaf;

  bool _writer;

  int remove (u_int depth, merkle_hash &key, merkle_node *n);
  int insert (u_int depth, merkle_hash &key, merkle_node *n);
  merkle_node *make_node (u_int32_t pointer);
  merkle_node *make_node (u_int32_t block_no, MERKLE_DISK_TYPE type);
  u_int32_t alloc_free_block (MERKLE_DISK_TYPE type);
  void free_block (u_int32_t block_no, MERKLE_DISK_TYPE type);
  void write_metadata ();
  void leaf2internal (uint depth, merkle_node_disk *n);
  void switch_root (merkle_node_disk *n);

  void init ();
  void close ();

public:
  vec<merkle_hash> database_get_keys (u_int depth,
					      const merkle_hash &prefix);
  bool key_exists (chordID key);
  void get_keyrange_nowrap (const chordID &min,
      const chordID &max, u_int n, vec<chordID> &keys) {}
  vec<chordID> get_keyrange (chordID min, chordID max, u_int n);

  merkle_tree_disk (str index, str internal, str leaf, bool writer = false);
  merkle_tree_disk (str path, bool writer = false);
  ~merkle_tree_disk ();
  int insert (merkle_hash &key);
  merkle_node *get_root ();
  merkle_node *lookup_exact (u_int depth, const merkle_hash &key);
  merkle_node *lookup (u_int depth, const merkle_hash &key);
  merkle_node *lookup (u_int *depth, u_int max_depth, const merkle_hash &key);
  void lookup_release (merkle_node *n);
  int remove (merkle_hash &key);
  void sync (bool reopen);
};

#endif /* _MERKLE_TREE_DISK_H_ */
