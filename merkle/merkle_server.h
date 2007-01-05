#ifndef _MERKLE_SERVER_H_
#define _MERKLE_SERVER_H_

class merkle_tree;
class user_args;
class getkeys_arg;
class getkeys_res;
class sendnode_arg;
class sendnode_res;

// One merkle_server runs for each node of the Chord ring.
//  - i.e., one merkle_server per virtual node
//  - and remember, each virtual node has own database 

class merkle_server {
 public:
  ptr<merkle_tree> ltree; // local tree

  void dispatch (user_args *a);
  merkle_server (ptr<merkle_tree> ltree);
  void handle_get_keys (getkeys_arg *arg, getkeys_res *res);
  void handle_send_node (sendnode_arg *arg, sendnode_res *res);

  static void handle_get_keys (ptr<merkle_tree> ltree,
      getkeys_arg *arg, getkeys_res *res);
  static void handle_send_node (ptr<merkle_tree> ltree,
      sendnode_arg *arg, sendnode_res *res);
};


#endif /* _MERKLE_SERVER_H_ */
