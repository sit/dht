// {{{ headers
/*
 * Copyright (c) 2003 Thomer M. Gil
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

#include "p2psim/p2protocol.h"
#include "consistenthash.h"
#include <list>
using namespace std;

// }}}
// {{{ class k_nodeinfo
class k_nodeinfo {
public:
  typedef ConsistentHash::CHID NodeID;
  k_nodeinfo(NodeID, IPAddress);
  NodeID id;
  IPAddress ip;
  Time firstts; // when we saw it first
  Time lastts;  // when we saw it last

  void checkrep() const;
};
// }}}
// {{{ class Kademlia
class k_bucket;
class Kademlia : public P2Protocol {
// {{{ public
public:
  class older;
  typedef ConsistentHash::CHID NodeID;
  typedef set<k_nodeinfo*, older> nodeinfo_set;
  Kademlia(Node*, Args);
  ~Kademlia() {}

  string proto_name() { return "Kademlia"; }
  virtual void join(Args*);
  virtual void crash(Args*) {}
  virtual void lookup(Args*);

  //
  // functors
  //
  class older { public:
    bool operator()(const k_nodeinfo* p1, const k_nodeinfo* p2) const {
      if(p1->id == p2->id)
        return false;
      if(p1->lastts == p2->lastts)
        return p1->id <  p2->id;
      return p1->lastts < p2->lastts;
    }
  };

  class younger { public:
    bool operator()(const k_nodeinfo* p1, const k_nodeinfo* p2) const {
      if(p1->id == p2->id)
        return false;
      if(p1->lastts == p2->lastts)
        return p1->id >  p2->id;
      return p1->lastts > p2->lastts;
    }
  };

  class closer { public:
    bool operator()(const k_nodeinfo* p1, const k_nodeinfo* p2) const {
      if(p1->id == p2->id)
        return false;
      Kademlia::NodeID dist1 = Kademlia::distance(p1->id, n);
      Kademlia::NodeID dist2 = Kademlia::distance(p2->id, n);
      if(dist1 == dist2)
        return p1->id < p2->id;
      return dist1 < dist2;
    }
    static NodeID n;
  };

  //
  // static utility methods
  //
  static string printbits(NodeID);
  static string printID(NodeID id);
  static NodeID distance(const NodeID, const NodeID);
  static unsigned common_prefix(NodeID, NodeID);
  static unsigned getbit(NodeID, unsigned);

  //
  // non-static utility methods
  //
  void do_lookup_wrapper(k_nodeinfo*, NodeID, set<k_nodeinfo*> * = 0);

  //
  // observer methods
  //
  NodeID id() const { return _id; }
  k_bucket *root() { return _root; }
  bool stabilized(vector<NodeID>*);

  //
  // RPC arguments
  // 
  // {{{ lookup_args and lookup_result
  struct lookup_args {
    lookup_args(NodeID xid, IPAddress xip, NodeID k = 0, bool b = false) :
      id(xid), ip(xip), key(k), controlmsg(b) {};
    NodeID id;
    IPAddress ip;
    NodeID key;
    bool controlmsg; // whether or not to count this as control overhead
  };

  struct lookup_result {
    nodeinfo_set results;
    NodeID rid;     // the guy who's replying
  };
  // }}}

  //
  // RPCable methods
  //
  void do_lookup(lookup_args*, lookup_result*);
  void find_node(lookup_args*, lookup_result*);


  //
  // set methods
  //

  // hack to pre-initialize k-buckets
  void init_state(list<Protocol*>);
  void reschedule_stabilizer(void*);
  friend class k_bucket;
  void setroot(k_bucket *k) { _root = k; }

  //
  // member variables
  //
  static unsigned k;                    // number of nodes per k-bucket
  static unsigned alpha;                // alpha from kademlia paper; no of simultaneous RPCs
  static unsigned debugcounter;         // 
  static unsigned joined;               // how many have joined so far
  static unsigned stabilize_timer;      // how often to stabilize
  static unsigned refresh_rate;         // how often to refresh info
  static const unsigned idsize = 8*sizeof(NodeID);
  hash_map<NodeID, k_nodeinfo*> flyweight;
// }}}
// {{{ private
private:
  void insert(NodeID, IPAddress, bool = false);
  void touch(NodeID);
  void erase(NodeID);
  void stabilize();

  NodeID _id;           // my id
  k_nodeinfo *_me;      // info about me
  k_bucket *_root;

  // statistics
  static unsigned controlmsg;

  //
  // utility 
  //
  class callinfo { public:
    callinfo(IPAddress xip, lookup_args *xla, lookup_result *xlr)
      : ip(xip), la(xla), lr(xlr) {}
    ~callinfo() { delete la; delete lr; }
    IPAddress ip;
    lookup_args *la;
    lookup_result *lr;
  };

// }}}
};

// }}}
// {{{ class k_bucket 
class k_traverser;
class k_bucket {
public:
  k_bucket(k_bucket*, bool, Kademlia * = 0);

  Kademlia *kademlia()  { return _kademlia; }
  void traverse(k_traverser*, string = "", unsigned = 0);
  void insert(Kademlia::NodeID, bool = false, string = "", unsigned = 0);
  void erase(Kademlia::NodeID, string = "", unsigned = 0);
  virtual void checkrep() const;

  bool leaf;

protected:
  k_bucket *parent;

private:
  Kademlia *_kademlia;
};
// }}}
// {{{ class k_bucket_node
class k_bucket_node : public k_bucket {
public:
  k_bucket_node(k_bucket *);
  k_bucket_node(Kademlia *);
  k_bucket *child[2];
  virtual void checkrep() const;
};
// }}}
// {{{ class k_bucket_leaf
class k_nodes;
class k_bucket_leaf : public k_bucket {
public:
  k_bucket_leaf(Kademlia *);
  k_bucket_leaf(k_bucket *);
  k_bucket_node* divide(unsigned);
  virtual void checkrep() const;

  k_nodes *nodes;
  set<k_nodeinfo*, Kademlia::younger> *replacement_cache;
};
// }}}
// {{{ class k_nodes
class k_bucket_leaf;

/*
 * keeps a sorted set of nodes.  the size of the set never exceeds Kademlia::k.
 */
class k_nodes {
public:
  typedef set<k_nodeinfo*, Kademlia::older> nodeset_t;
  k_nodes(k_bucket_leaf *parent);
  void insert(Kademlia::NodeID);
  void erase(Kademlia::NodeID);
  bool contains(Kademlia::NodeID) const;
  bool inrange(Kademlia::NodeID) const;
  bool full() const { return nodes.size() >= Kademlia::k; }
  bool empty() const { return !nodes.size(); }
  void checkrep(bool = true) const;

  nodeset_t nodes;

private:
  k_bucket_leaf *_parent;
};
// }}}

// {{{ class k_traverser
class k_traverser { public:
  virtual void execute(k_bucket_leaf*, string, unsigned) = 0;
};
// }}}
// {{{ class k_collect_closest
class k_collect_closest : public k_traverser { public:
  typedef set<k_nodeinfo*, Kademlia::closer> resultset_t;
  k_collect_closest(Kademlia::NodeID, unsigned best = Kademlia::k);
  virtual ~k_collect_closest() {}
  virtual void execute(k_bucket_leaf *, string, unsigned);

  resultset_t results;

private:
  void checkrep();
  Kademlia::NodeID _node;
  Kademlia::NodeID _lowest;
  unsigned _best;
};
// }}}
// {{{ class k_stabilizer
class k_stabilizer : public k_traverser { public:
  k_stabilizer() {}
  virtual ~k_stabilizer() {}
  virtual void execute(k_bucket_leaf *, string, unsigned);
};
// }}}
// {{{ class k_stabilized
class k_stabilized : public k_traverser { public:
  k_stabilized(vector<Kademlia::NodeID> *v) : _v(v), _stabilized(true) {}

  virtual ~k_stabilized() {}
  virtual void execute(k_bucket_leaf*, string, unsigned);
  bool stabilized() { return _stabilized; }

private:
  vector<Kademlia::NodeID> *_v;
  bool _stabilized;
};
// }}}

#define KDEBUG(x) DEBUG(x) << Kademlia::debugcounter++ << "(" << now() << "). " << Kademlia::printbits(_id) << " "
#endif // __KADEMLIA_H
