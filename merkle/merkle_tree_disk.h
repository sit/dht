#ifndef _MERKLE_TREE_DISK_H_
#define _MERKLE_TREE_DISK_H_

#include "async.h"
#include "merkle_tree.h"

class merkle_node_disk : public merkle_node {

 private:
  array<merkle_node_disk, 64> *entry_disk;

 public:

  merkle_node *child (u_int i);
  void leaf2internal ();
  void internal2leaf ();
  bool isleaf ();
  merkle_node_disk ();
  ~merkle_node_disk ();

};

class merkle_tree_disk : public merkle_tree {
private:

  void remove (u_int depth, merkle_hash &key, merkle_node *n);
  int insert (u_int depth, merkle_hash &key, merkle_node *n);
  merkle_node *lookup (u_int *depth, u_int max_depth, 
		       const merkle_hash &key, merkle_node *n);

public:
  merkle_tree_disk ();
  ~merkle_tree_disk ();

};




#endif /* _MERKLE_TREE_DISK_H_ */
