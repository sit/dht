#ifndef __KADEMLIA_H
#define __KADEMLIA_H

#include "protocol.h"
#include "node.h"
#include <map>
#include <set>
#include <iostream>
using namespace std;

extern unsigned kdebugcounter;
class k_bucket_tree;

// {{{ Kademlia
class Kademlia : public Protocol {
// {{{ public
public:
  typedef short NodeID;
  typedef unsigned Value;
  static const unsigned idsize = 8*sizeof(NodeID);

  Kademlia(Node*);
  ~Kademlia();

  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void insert(Args*);
  virtual void lookup(Args*);

  static string printbits(NodeID);
  static string printID(NodeID id);
  static NodeID distance(NodeID, NodeID);

  bool stabilized(vector<NodeID>);
  void dump();
  NodeID id () { return _id;}

  // bit twiddling utility functions
  static NodeID flipbitandmaskright(NodeID, unsigned);
  static NodeID maskright(NodeID, unsigned);
  static unsigned getbit(NodeID, unsigned);
  static unsigned k()   { return _k; }

  pair<NodeID, IPAddress> do_lookup_wrapper(IPAddress, NodeID);

  // public, because k_bucket needs it.
  struct lookup_args {
    NodeID id;
    IPAddress ip;

    NodeID key;
  };

  struct lookup_result {
    NodeID id;      // answer to the lookup
    IPAddress ip;   // answer to the lookup
    NodeID rid;     // the guy who's replying
  };
// }}}
// {{{ private
private:
  static unsigned _k;           // k from kademlia paper
  NodeID _id;                   // my id
  k_bucket_tree *_tree;         // the root of our k-bucket tree
  map<NodeID, Value> _values;   // key/value pairs

  static NodeID _rightmasks[]; // for bitfucking

  void reschedule_stabilizer(void*);
  void stabilize();
  // {{{ structs
  //
  // join
  //
  struct join_args {
    NodeID id;
    IPAddress ip;
  };
  struct join_result {
    int ok;
  };
  void do_join(void *args, void *result);


  //
  // lookup
  //
  void do_lookup(void *args, void *result);


  //
  // insert
  //
  struct insert_args {
    NodeID id;
    IPAddress ip;

    NodeID key;
    Value val;
  };
  struct insert_result {
  };
  void do_insert(void *args, void *result);


  //
  // transfer
  //
  class fingers_t;
  struct transfer_args {
    NodeID id;
    IPAddress ip;
  };
  struct transfer_result {
    map<NodeID, Value> values;
  };
  void do_transfer(void *args, void *result);
  // }}}
// }}}
};

// }}}
// {{{ peer_t
// one entry in k_bucket's _nodes vector
class peer_t {
public:
  typedef Kademlia::NodeID NodeID;

  peer_t(NodeID xid, IPAddress xip, Time t) : retries(0), id(xid), ip(xip), lastts(t) {}
  peer_t(const peer_t &p) : retries(0), id(p.id), ip(p.ip), lastts(p.lastts) {}
  unsigned retries;
  NodeID id;
  IPAddress ip;
  Time lastts;
};

// }}}
// {{{ k-bucket
class k_bucket {
public:
  typedef Kademlia::NodeID NodeID;

  k_bucket(Kademlia*);
  ~k_bucket();

  peer_t *random_node();

  pair<peer_t*, unsigned>
    insert(NodeID, IPAddress, NodeID = 0, unsigned = 0, k_bucket* = 0);
  bool stabilized(vector<NodeID>, string = "", unsigned = 0);
  void stabilize(string = "", unsigned = 0);
  void dump(string = "", unsigned = 0);
  peer_t* get(NodeID, unsigned = 0);

private:
  k_bucket* _child[2];          // subtree
  vector<peer_t*> _nodes;       // all nodes
  static unsigned _k;

  Kademlia *_self;
  NodeID _id; // so that KDEBUG() works. can be removed later.
};


// }}}
// {{{ k_bucket_tree
class k_bucket_tree {
public:
  typedef Kademlia::NodeID NodeID;

  k_bucket_tree(Kademlia*);
  ~k_bucket_tree();
  unsigned insert(NodeID node, IPAddress ip);
  bool stabilized(vector<NodeID>);
  void stabilize();
  void dump() { return _root->dump(); }
  bool empty() { return _nodes.empty(); }
  pair<NodeID, IPAddress> get(NodeID);

private:
  k_bucket *_root;
  set<peer_t*> _nodes;

  Kademlia *_self;
  NodeID _id; // so that KDEBUG() does work
};

// }}}

#define KDEBUG(x) DEBUG(x) << kdebugcounter++ << ". " << Kademlia::printbits(_id) << " "

#endif // __KADEMLIA_H
