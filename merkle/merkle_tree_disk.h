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

struct merkle_internal_node {
  merkle_char_key hashes[64];
  uint32 child_pointers[64];
  uint32 key_count;
};

struct merkle_leaf_node {
  merkle_char_key keys[64];
  uint32 key_count;
};

class merkle_node_disk : public merkle_node {

 private:
  itree<chordID, merkle_key, &merkle_key::id, &merkle_key::ik> keylist;
  array<merkle_hash, 64> *hashes;
  array<uint32, 64> *children;

  FILE *_index;
  FILE *_internal;
  FILE *_leaf;
  MERKLE_DISK_TYPE _type;
  uint32 _block_no;

 public:

  merkle_node *child (u_int i);
  void leaf2internal ();
  void internal2leaf ();
  bool isleaf ();
  void add_key( chordID key );
  void write_out();
  merkle_node_disk (FILE *index, FILE *internal, FILE *leaf, 
		    MERKLE_DISK_TYPE type, uint32 block_no);
  ~merkle_node_disk ();

};

class merkle_tree_disk : public merkle_tree {
private:

  str _index_name;
  str _internal_name;
  str _leaf_name;

  FILE *_index;
  FILE *_internal;
  FILE *_leaf;

  void remove (u_int depth, merkle_hash &key, merkle_node *n);
  int insert (u_int depth, merkle_hash &key, merkle_node *n);
  merkle_node *lookup (u_int *depth, u_int max_depth, 
		       const merkle_hash &key, merkle_node *n);
  merkle_node *make_node( uint32 pointer );

public:
  merkle_tree_disk (str index, str internal, str leaf);
  ~merkle_tree_disk ();

};




#endif /* _MERKLE_TREE_DISK_H_ */
