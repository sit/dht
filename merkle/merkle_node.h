#ifndef _MERKLE_NODE_H_
#define _MERKLE_NODE_H_

#include "merkle_hash.h"
#include "merkle_misc.h"

class merkle_node {
private:
  array<merkle_node, 64> *entry; 
public:
  u_int64_t count;
  merkle_hash hash;

  virtual merkle_hash child_hash (u_int i);
  virtual merkle_node *child (u_int i);
  virtual bool isleaf () const;
  bool leaf_is_full () const;
  virtual void internal2leaf ();
  virtual void leaf2internal ();
  void dump (u_int depth);

  merkle_node ();
  void initialize (u_int64_t _count);
  virtual ~merkle_node ();
};

#endif /* _MERKLE_NODE_H_ */
