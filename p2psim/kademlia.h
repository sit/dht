#ifndef __KADEMLIA_H
#define __KADEMLIA_H

#include "dhtprotocol.h"
#include "node.h"
#include <map>
#include <set>
#include <iostream>
using namespace std;

extern unsigned kdebugcounter;
class k_bucket_tree;
class peer_t;

// {{{ Kademlia
class Kademlia : public DHTProtocol {
// {{{ public
public:
  typedef unsigned short NodeID;
  typedef unsigned Value;
  static const unsigned idsize = 8*sizeof(NodeID);

  Kademlia(Node*, Args);
  ~Kademlia();
  string proto_name() { return "Kademlia"; }

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
  // static NodeID flipbitandmaskright(NodeID, unsigned);
  // static NodeID maskright(NodeID, unsigned);
  static unsigned getbit(NodeID, unsigned);
  static unsigned common_prefix(NodeID, NodeID);
  static peer_t* get_closest(vector<peer_t*> *, NodeID);
  static unsigned k()   { return _k; }

  void do_lookup_wrapper(peer_t*, NodeID, vector<peer_t*> * = 0);

  // public, because k_bucket needs it.
  struct lookup_args {
    lookup_args(NodeID xid, IPAddress xip, NodeID k = 0) :
      id(xid), ip(xip), key(k) {};
    NodeID id;
    IPAddress ip;
    NodeID key;
  };

  struct lookup_result {
    // NodeID id;      // answer to the lookup
    // IPAddress ip;   // answer to the lookup
    vector<peer_t*> results;
    NodeID rid;     // the guy who's replying
  };

  //
  // ping
  //
  struct ping_args {
    ping_args(NodeID xid, IPAddress xip) : id(xid), ip(xip) {}
    NodeID id;
    IPAddress ip;
  };
  struct ping_result {};
  void do_ping(ping_args*, ping_result*);
  bool do_ping_wrapper(peer_t*);

// }}}
// {{{ private
 private:
  static unsigned _k;           // k from kademlia paper
  static unsigned _alpha;       // alpha from kademlia paper
  NodeID _id;                   // my id
  k_bucket_tree *_tree;         // the root of our k-bucket tree
  map<NodeID, Value> _values;   // key/value pairs
  IPAddress _wkn;               // well-known IP address
  bool _joined;                 // have I joined yet?  REMOVE ME.

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
  void do_lookup(lookup_args *largs, lookup_result *lresult);


  //
  // insert
  //
  struct insert_args {
    NodeID id;      // node doing the insert
    IPAddress ip;   // node doing the insert

    NodeID key;
    Value val;
  };
  struct insert_result {
  };
  void do_insert(insert_args *args, insert_result *result);

  //
  // transfer
  //
  class fingers_t;
  struct transfer_args {
    transfer_args(NodeID xid, IPAddress xip) : id(xid), ip(xip) {}
    NodeID id;
    IPAddress ip;
  };
  struct transfer_result {
    map<NodeID, Value> *values;
  };
  void do_transfer(transfer_args *targs, transfer_result *tresult);

  void _tree_insert(peer_t&);
  // }}}
// }}}
};

// }}}
// {{{ peer_t
// one entry in k_bucket's _nodes vector
class peer_t {
public:
  typedef Kademlia::NodeID NodeID;

  peer_t(NodeID xid, IPAddress xip, Time t = now())
    : retries(0), id(xid), ip(xip), firstts(t), lastts(t) {}
  peer_t(const peer_t &p) : retries(0), id(p.id), ip(p.ip), lastts(p.lastts) {}
  unsigned retries;
  NodeID id;
  IPAddress ip;
  Time firstts; // when we saw it first
  Time lastts;  // when we saw it last
};

// }}}
// {{{ k-bucket
class k_bucket {
public:
  typedef Kademlia::NodeID NodeID;

  k_bucket(Kademlia*, k_bucket_tree*);
  ~k_bucket();

  peer_t* insert(NodeID, IPAddress, string = "", unsigned = 0, k_bucket* = 0);
  bool stabilized(vector<NodeID>, string = "", unsigned = 0);
  void stabilize(string = "", unsigned = 0);
  void dump(string = "", unsigned = 0);
  // void get(NodeID, vector<pair<NodeID, IPAddress> >*, unsigned = 0);

private:
  static unsigned _k;
  bool _leaf;                   // this should/can not be split further
  Kademlia *_self;              // the kademlia node that this tree is part of
  k_bucket_tree *_root;         // root of the tree that I'm a part of

  // the following are mutually exclusive, they could go into a union.
  class SortedByLastTime { public:
    bool operator()(const peer_t* p1, const peer_t* p2) {
      return p1->lastts < p2->lastts;
    }
  };
  set<peer_t*, SortedByLastTime> *_nodes;      // for a leaf
  k_bucket* _child[2];          // for a node

  NodeID _id; // so that KDEBUG() works. can be removed later.
};

// }}}
// {{{ k_bucket_tree
class k_bucket_tree {
public:
  typedef Kademlia::NodeID NodeID;

  k_bucket_tree(Kademlia*);
  ~k_bucket_tree();
  void insert(NodeID node, IPAddress ip);
  void insert(vector<peer_t*>*);
  void erase(NodeID node);
  bool stabilized(vector<NodeID>);
  void stabilize();
  void dump() { return _root->dump(); }
  bool empty() { return _nodes.empty(); }
  void get(NodeID, vector<peer_t*>*);
  peer_t* random_node();


private:
  class SortNodes { public:
    SortNodes(NodeID key) : _key(key) {}
    bool operator()(const peer_t* n1, const peer_t* n2) const {
      // cout << "compare " << Kademlia::printbits(_key) << " ^ " << Kademlia::printbits(n1.first) << " = " << Kademlia::printbits(_key ^ n1.first) << " with " << Kademlia::printbits(_key) << " ^ " << Kademlia::printbits(n2.first) << " = " << Kademlia::printbits(_key ^ n2.first) << endl;
      return (_key ^ n1->id) < (_key ^ n2->id);
    }
  private:
    NodeID _key;
  };

  k_bucket *_root;
  map<NodeID, peer_t*> _nodes;

  Kademlia *_self;
  NodeID _id; // so that KDEBUG() does work
  static unsigned _k;
};

// }}}

#define KDEBUG(x) DEBUG(x) << kdebugcounter++ << ". " << Kademlia::printbits(_id) << " "

#endif // __KADEMLIA_H
