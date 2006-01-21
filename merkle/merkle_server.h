#ifndef _MERKLE_SERVER_H_
#define _MERKLE_SERVER_H_

class merkle_tree;
class user_args;

// One merkle_server runs for each node of the Chord ring.
//  - i.e., one merkle_server per virtual node
//  - and remember, each virtual node has own database 

class merkle_server {
 public:
  merkle_tree *ltree; // local tree

  void dispatch (user_args *a);
  merkle_server (merkle_tree *ltree);

};


#endif /* _MERKLE_SERVER_H_ */
