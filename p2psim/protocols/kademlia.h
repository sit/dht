/*
 * Copyright (c) 2003 [NAMES_GO_HERE]
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef __KADEMLIA_H
#define __KADEMLIA_H

#include "chord.h"

extern unsigned kdebugcounter;
class k_bucket_tree;
class peer_t;

// {{{ Kademlia
class Kademlia : public DHTProtocol {
// {{{ public
public:
  typedef ConsistentHash::CHID NodeID;
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
  static NodeID distance(const NodeID, const NodeID);

  void init_state(list<Protocol*>);

// }}}
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
    lookup_args(NodeID xid, IPAddress xip, NodeID k = 0, bool b = false) :
      id(xid), ip(xip), key(k), controlmsg(b) {};
    NodeID id;
    IPAddress ip;
    NodeID key;
    bool controlmsg; // whether or not to count this as control overhead
  };

  struct lookup_result {
    vector<peer_t*> results;
    NodeID rid;     // the guy who's replying
  };
  void find_node(lookup_args*, lookup_result*);

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
  hash_map<NodeID, Value> _values;   // key/value pairs
  peer_t *_me;
  static unsigned _joined;      // how many have joined

  // statistics
  static unsigned _controlmsg;  //

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
    hash_map<NodeID, Value> *values;
  };
  void do_transfer(transfer_args *targs, transfer_result *tresult);

  void _tree_insert(peer_t&);

  class callinfo { public:
    callinfo(IPAddress xip, lookup_args *xla, lookup_result *xlr)
      : ip(xip), la(xla), lr(xlr) {}
    ~callinfo() { delete la; delete lr; }
    IPAddress ip;
    lookup_args *la;
    lookup_result *lr;
  };

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
// {{{ SortNodes
class SortNodes { public:
  SortNodes(Kademlia::NodeID key) : _key(key) {}
  bool operator()(const peer_t* n1, const peer_t* n2) const {
    Kademlia::NodeID dist1 = _key ^ n1->id;
    Kademlia::NodeID dist2 = _key ^ n2->id;
    return dist1 < dist2;
  }
private:
  Kademlia::NodeID _key;
};


class EqualNodes { public:
  EqualNodes(Kademlia::NodeID key) : _key(key) {}
  bool operator()(const peer_t* n1, const peer_t* n2) const {
    Kademlia::NodeID dist1 = _key ^ n1->id;
    Kademlia::NodeID dist2 = _key ^ n2->id;
    return dist1 == dist2;
  }
private:
  Kademlia::NodeID _key;
};



// }}}
// {{{ k-bucket
class k_bucket {
public:
  typedef Kademlia::NodeID NodeID;

  k_bucket(Kademlia*, k_bucket_tree*);
  ~k_bucket();

  peer_t* insert(NodeID, IPAddress, bool = false, string = "", unsigned = 0, k_bucket* = 0);
  void k_bucket::erase(NodeID, IPAddress, string = "", unsigned = 0);
  bool stabilized(vector<NodeID>, string = "", unsigned = 0);
  void stabilize(string = "", unsigned = 0);
  void dump(string = "", unsigned = 0);
  // void get(NodeID, vector<pair<NodeID, IPAddress> >*, unsigned = 0);

private:
  static unsigned _k;
  bool _leaf;                   // this should/can not be split further
  Kademlia *_self;              // the kademlia node that this tree is part of
  k_bucket_tree *_root;         // root of the tree that I'm a part of
  NodeID _id;                   // XXX: so that KDEBUG() works. can be removed later.

  /*
   * LEAFS
   */
  class OldestFirst { public:
    bool operator()(const peer_t* p1, const peer_t* p2) {
      return p1->lastts != p2->lastts ?
             p1->lastts < p2->lastts :
             p1 < p2;
    }
  };
  set<peer_t*, OldestFirst> *_nodes;

  class NewestFirst { public:
    bool operator()(const peer_t* p1, const peer_t* p2) {
      return p1->lastts != p2->lastts ?
             p1->lastts > p2->lastts :
             p1 > p2;
    }
  };
  set<peer_t*, NewestFirst> *_replacement_cache;

  /*
   * NON-LEAFS
   */
  k_bucket* _child[2];          // for a node
};

// }}}
// {{{ k_bucket_tree
class k_bucket_tree {
public:
  typedef Kademlia::NodeID NodeID;

  k_bucket_tree(Kademlia*);
  ~k_bucket_tree();
  void insert(NodeID node, IPAddress ip, bool = false);
  void insert(vector<peer_t*>*);
  void erase(NodeID node);
  bool stabilized(vector<NodeID>);
  void stabilize();
  void dump() { return _root->dump(); }
  bool empty() { return _nodes.empty(); }
  void get(NodeID, vector<peer_t*>*, unsigned best = _k);
  peer_t* random_node();


private:
  k_bucket *_root;
  hash_map<NodeID, peer_t*> _nodes;

  // best_entry
  struct best_entry {
    best_entry() { dist = 0; peers.clear(); }
    NodeID dist;
    vector<peer_t*> peers;
    sklist_entry<best_entry> _sortlink;
  };

  // must return same results as SortNodes
  struct DistCompare {
    int operator()(const NodeID &n1, const NodeID &n2) const {
      NodeID dist1 = Kademlia::distance(n1, _key);
      NodeID dist2 = Kademlia::distance(n2, _key);
      return dist1 < dist2;
    }
    static NodeID _key;
  };

  Kademlia *_self;
  NodeID _id; // so that KDEBUG() does work
  static unsigned _k;
};

// }}}

#define KDEBUG(x) DEBUG(x) << kdebugcounter++ << "(" << now() << "). " << Kademlia::printbits(_id) << " "

#endif // __KADEMLIA_H
