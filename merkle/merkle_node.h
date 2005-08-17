#ifndef _MERKLE_NODE_H_
#define _MERKLE_NODE_H_

#include "async.h"
#include "sha1.h"
#include "merkle_hash.h"
#include "merkle_misc.h"
#include "libadb.h"

class merkle_node {
private:
  array<merkle_node, 64> *entry; 
public:
  u_int64_t count;
  merkle_hash hash;

  const merkle_node *child (u_int i) const;
  merkle_node *child (u_int i);
  bool isleaf () const;
  bool leaf_is_full () const;
  void internal2leaf ();
  void leaf2internal ();
  void dump (u_int depth) const;

  merkle_node ();
  void initialize (u_int64_t _count);
  ~merkle_node ();
  void check_invariants (u_int depth, merkle_hash prefix, ptr<adb> db);
};




#endif /* _MERKLE_NODE_H_ */
