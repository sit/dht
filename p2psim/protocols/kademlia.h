// {{{ headers
/*
 * Copyright (c) 2003-2005 Thomer M. Gil
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
#include <iostream>
#include "p2psim/bighashmap.hh"
using namespace std;

// }}}
// {{{ class k_nodeinfo
class k_nodeinfo {
public:
  typedef ConsistentHash::CHID NodeID;
  k_nodeinfo() { id = 0; ip = 0; lastts = 0; timeouts = 0; RTT = 0;}
  k_nodeinfo(NodeID, IPAddress, Time = 0);
  k_nodeinfo(k_nodeinfo*);
  NodeID id;
  IPAddress ip;
  Time lastts;   // last time we know it was alive

  unsigned timeouts; // how often we did not get a reply
  Time RTT;

  inline void checkrep() const;
};
// }}}
// {{{ class Kademlia
class k_bucket;
class Kademlia : public P2Protocol {
public:
  typedef ConsistentHash::CHID NodeID;

private:
// class k_nodeinfo_pool {{{
  struct k_nodeinfo_buffer
  {
    k_nodeinfo_buffer() {
      ki = 0;
      next = prev = 0;
    }

    ~k_nodeinfo_buffer() {
      delete ki;
    }

    k_nodeinfo *ki;
    k_nodeinfo_buffer *next;
    k_nodeinfo_buffer *prev;
  };

  class k_nodeinfo_pool
  {
  public:
    k_nodeinfo_pool() {
      _head = _tail = New k_nodeinfo_buffer;
      _count = 0;
    }

    ~k_nodeinfo_pool() {
      k_nodeinfo_buffer *i = _head;
      while(i) {
        k_nodeinfo_buffer *next = i->next;
        delete i;
        i = next;
      }
    }

    k_nodeinfo* pop(Kademlia::NodeID id, IPAddress ip, Time RTT, char timeouts)
    {
      if(!_count)
        return New k_nodeinfo(id, ip, RTT);

      k_nodeinfo *newki = _pop();
      newki->id = id;
      newki->ip = ip;
      newki->lastts = 0;
      newki->timeouts = timeouts;
      newki->RTT = RTT;
      _count--;
      return newki;
    }

    void push(k_nodeinfo *ki)
    {
      // verify that this pointer isn't in here yet.
      if(Kademlia::docheckrep) {
        k_nodeinfo_buffer *i = _head;
        do {
          assert(i->ki != ki);
        } while(i->next == 0);
      }

      // allocate new space
      if(_tail->next == 0) {
        _tail->next = New k_nodeinfo_buffer;
        _tail->next->prev = _tail;
      }

      _tail = _tail->next;
      _tail->ki = ki;
      _count++;
      ki->ip = 0;
      ki->id = 0;
      ki->lastts = 0;
      ki->timeouts = 0;
      ki->RTT = 0;
    }

  private:
    k_nodeinfo* _pop()
    {
      _tail = _tail->prev;
      return _tail->next->ki;
    }

  private:
    unsigned _count;
    k_nodeinfo_buffer* _head;
    k_nodeinfo_buffer* _tail;
  };
// }}}
// {{{ public
public:
  Kademlia(IPAddress, Args);
  ~Kademlia();

  virtual void join(Args*);
  virtual void crash(Args*);
  virtual void lookup(Args*);
  virtual void initstate();
  virtual void nodeevent (Args *) {};

  //
  // functors
  //
  class closer { public:
    bool operator()(const k_nodeinfo p1, const k_nodeinfo p2) const {
      if(p1.id == p2.id)
        return false;
      Kademlia::NodeID dist1 = Kademlia::distance(p1.id, n);
      Kademlia::NodeID dist2 = Kademlia::distance(p2.id, n);
      if(dist1 == dist2)
        return p1.id < p2.id;
      return dist1 < dist2;
    }
    static NodeID n;
  };

  //
  // same as closer, but takes RTT into account
  //
  class closerRTT { public:
    bool operator()(const k_nodeinfo p1, const k_nodeinfo p2) const {
      if(p1.id == p2.id)
        return false;
      unsigned cp1 = Kademlia::common_prefix(p1.id, n);
      unsigned cp2 = Kademlia::common_prefix(p2.id, n);
      if(cp1 == cp2) {
        if(p1.RTT == p2.RTT) {
          return p1.id < p2.id;

        // prefer non-zero over zero
        } else if(p1.RTT == 0 && p2.RTT != 0) {
          return false;
        } else if(p1.RTT != 0 && p2.RTT == 0) {
          return true;
        } else {
          return p1.RTT < p2.RTT;
        }
      }
      return cp1 > cp2;
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
  static void reap(void*);

  //
  // observer methods
  //
  NodeID id() const { return _id; }
  k_bucket *root() { return _root; }
  bool stabilized(vector<NodeID>*);

  // statistics
  enum stat_type {
    STAT_LOOKUP = 0,
    STAT_STABILIZE,
    STAT_JOIN,
    STAT_PING,
    STAT_ERASE
  };

  //
  // RPC arguments
  // 
  // {{{ find_value_args and find_value_args
  struct find_value_args {
    find_value_args(NodeID xid, IPAddress xip, NodeID k = 0) :
      id(xid), ip(xip), key(k), stattype(STAT_LOOKUP) {}
    NodeID id;
    IPAddress ip;
    NodeID key;
    stat_type stattype; // for stats
  };

  class find_value_result { public:
    find_value_result() { rpcs = hops = timeouts = 0; latency = 0; }
    ~find_value_result() { }
    k_nodeinfo succ;
    NodeID rid;
    
    // statistics
    unsigned rpcs;      // total number of RPCs we sent
    unsigned hops;      // number of hops for lookups
    unsigned timeouts;  // number of !ok replies
    Time latency;       // latency from nodes that make hopcount go up
  };
  // }}}
  // {{{ find_node_args and find_node_result
  struct find_node_args {
    find_node_args(NodeID xid, IPAddress xip, NodeID k) :
      id(xid), ip(xip), key(k), stattype(STAT_LOOKUP) {}
    NodeID id;
    IPAddress ip;
    NodeID key;
    stat_type stattype;
  };

  class find_node_result { public:
    find_node_result() {}
    vector<k_nodeinfo> results;
    unsigned hops;
    unsigned which_alpha;
    NodeID rid;
  };
  // }}}
  // {{{ lookup_args and lookup_result
  struct lookup_args {
    lookup_args(NodeID xid, IPAddress xip, NodeID k = 0, bool ri = false) :
      id(xid), ip(xip), key(k), stattype(STAT_LOOKUP) {}
    NodeID id;
    IPAddress ip;
    NodeID key;
    stat_type stattype;
  };

  class lookup_result { public:
    lookup_result() { results.clear(); hops = 0; }
    ~lookup_result() { }
    set<k_nodeinfo, closer> results;
    NodeID rid;     // the guy who's replying
    unsigned hops;
  };

  struct lookup_wrapper_args {
    IPAddress ipkey;
    NodeID key;
    Time starttime;
    unsigned attempts;
  };
  // }}}
  // {{{ ping_args and ping_result
  struct ping_args {
    ping_args(NodeID id, IPAddress ip) : id(id), ip(ip) {}
    NodeID id;
    IPAddress ip;
  };

  struct ping_result {
  };
  // }}}
  // {{{ erase_args and erase_result
  struct erase_args {
    erase_args(vector<NodeID> * ids) : ids(ids) {}
    vector<NodeID> *ids;
  };

  struct erase_result {
  };
  // }}}

  //
  // RPCable methods
  //
  void do_lookup(lookup_args*, lookup_result*);
  void find_value(find_value_args*, find_value_result*);
  void do_ping(ping_args*, ping_result*);
  void find_node(find_node_args*, find_node_result*);
  void do_erase(erase_args*, erase_result*);


  //
  // set methods
  //

  // hack to pre-initialize k-buckets
  void reschedule_stabilizer(void*);
  friend class k_bucket;

  // use_replacement_cache values:
  // DISABLED: no
  // ENABLED: yes, but not as aide in find_node
  // FULL: yes
  enum use_replacement_cache_t {
    DISABLED = 0,
    ENABLED,
    FULL
  };

  //
  // member variables
  //
  static bool docheckrep;
  static use_replacement_cache_t use_replacement_cache;
  static unsigned k;                    // number of nodes per k-bucket
  static unsigned k_tell;               // number of values returned by find_node
  static unsigned alpha;                // alpha from kademlia paper; no of simultaneous RPCs
  static unsigned debugcounter;         // 
  static unsigned stabilize_timer;      // how often to stabilize
  static unsigned refresh_rate;         // how often to refresh info
  static unsigned erase_count;          // after how many timeouts to remove
  static bool learn_stabilize_only;     // do we learn from RPCs?
  static bool force_stabilization;      // stabilize all buckets, not only old ones
  static bool death_notification;       // if your neighbor told you about a 
                                        // dead node, tell them about it.
  static Time max_lookup_time;          // how long do we keep retrying
  static Time _default_timeout;         // default timeout
  static unsigned _to_cheat;            // whether to use roundtrip estimates as timeout
  static unsigned _to_multiplier;       // the multiplier used to calculate timeout
  static k_nodeinfo_pool *pool;         // pool of k_nodeinfo_pool
  static const unsigned idsize = 8*sizeof(Kademlia::NodeID);
  HashMap<NodeID, k_nodeinfo*> flyweight;
// }}}
// {{{ private
private:
  Time timeout(IPAddress dst);      //how to calculate timeout for RPC
  void init_k_bucket_tree();       //jy: init k bucket tree root
  void insert(NodeID, IPAddress, Time = 0, char = 0, bool = false);
  void touch(NodeID);
  void erase(NodeID);
  void stabilize();

  NodeID _id;           // my id
  k_nodeinfo _me;      // info about me
  k_bucket *_root;
  bool _joined;

  void record_stat(stat_type, unsigned, unsigned);
  friend class k_stabilizer;
  void update_k_bucket(NodeID, IPAddress, Time = 0);
  void clear();
  void lookup_wrapper(lookup_wrapper_args*);

  // number of instantiated kademlias.
  static unsigned _nkademlias;

  // global statistics
  static long long unsigned _rpc_bytes;
  static long unsigned _good_rpcs;
  static long unsigned _bad_rpcs;
  static long unsigned _ok_by_reaper;
  static long unsigned _timeouts_by_reaper;
  static Time _time_spent_timeouts;

  static long unsigned _good_lookups;
  static long unsigned _good_attempts;
  static long unsigned _bad_attempts;
  static long unsigned _lookup_dead_node;
  static long unsigned _ok_failures;
  static long unsigned _bad_failures;

  static Time _good_total_latency;
  static Time _good_lookup_latency;
  static Time _good_ping_latency;
  static long unsigned _good_timeouts;

  static long unsigned _good_hops;
  static Time _good_hop_latency;

  static Time _bad_lookup_latency;
  static long unsigned _bad_timeouts;
  static long unsigned _bad_hops;
  static Time _bad_hop_latency;



  //
  // utility 
  //
  class callinfo { public:
    callinfo(k_nodeinfo ki, find_node_args *fa, find_node_result *fr)
      : ki(ki), fa(fa), fr(fr) { before = 0; }
    ~callinfo() {
      delete fa;
      delete fr;
    }
    k_nodeinfo ki;
    find_node_args *fa;
    find_node_result *fr;
    Time before;
  };

  struct reap_info {
    reap_info() {}
    ~reap_info() { 
      delete rpcset; 
      delete outstanding_rpcs; 
      delete who_told_me;
      delete is_dead;
    }

    Kademlia *k;
    RPCSet *rpcset;
    HashMap<unsigned, callinfo*>* outstanding_rpcs;
    HashMap<NodeID, vector<IPAddress> * > *who_told_me;
    HashMap<NodeID, bool> *is_dead;
    stat_type stat;
  };

  // hack for initstate
  static NodeID *_all_kademlias;
  static HashMap<NodeID, Kademlia*> *_nodeid2kademlia;
// }}}
};

// }}}
// {{{ class k_nodes
class k_bucket;

int k_nodeinfo_cmp(const void *k1, const void *k2);

/*
 * stores a set of k_nodeinfo*.  the size of the set formally never exceeds
 * Kademlia::k, but internally we let it grow bigger.  only once every so often
 * we truncate it.  (see get())
 */
class k_nodes {
public:
  k_nodes(k_bucket *parent);
  ~k_nodes();
  void insert(Kademlia::NodeID, bool);
  void erase(Kademlia::NodeID);
  bool contains(Kademlia::NodeID);
  bool inrange(Kademlia::NodeID);
  void clear();
  void checkrep();
  unsigned size()       { return _map.size() >= (int) Kademlia::k ? Kademlia::k : _map.size(); }
  k_nodeinfo* last()    { return get(size()-1); }
  bool full() const     { return _map.size() >= (int) Kademlia::k; }
  bool empty() const    { return !_map.size(); }
  k_nodeinfo* get(unsigned);

private:

  k_bucket *_parent;
  HashMap<k_nodeinfo*, bool> _map;

  // 
  k_nodeinfo **_nodes;
  enum redo_t {
    NOTHING = 0,
    RESORT,
    REBUILD
  };
  redo_t _redo; // 0: nothing, 1: resort, 2: refill & resort

  void rebuild();
};
// }}}
// {{{ class k_bucket 
class k_traverser;
class k_bucket {
public:
  k_bucket(k_bucket*, Kademlia*);
  ~k_bucket();

  Kademlia *kademlia()  { return _kademlia; }
  void traverse(k_traverser*, Kademlia*, string = "", unsigned = 0, unsigned = 0);
  void insert(Kademlia::NodeID, bool, bool = false, string = "", unsigned = 0);
  void erase(Kademlia::NodeID, string = "", unsigned = 0);
  void find_node(Kademlia::NodeID, vector<k_nodeinfo*>*, unsigned = Kademlia::k_tell, unsigned = 0);
  void checkrep();

  void divide(unsigned);
  void collapse();

  bool leaf;

  // in case we are a leaf
  k_nodes *nodes;
  k_nodes *replacement_cache;

  // in case are a node, i.e., not a leaf
  k_bucket *child[2];

protected:
  k_bucket *parent;

private:
  Kademlia *_kademlia;

};
// }}}

// {{{ class k_traverser
class k_traverser { public:
  k_traverser(string type = "") : _type(type) {}
  virtual ~k_traverser() {}
  virtual void execute(k_bucket*, string, unsigned, unsigned) = 0;
  string type() { return _type; };

private:
  string _type;
};
// }}}
// {{{ class k_stabilizer
class k_stabilizer : public k_traverser { public:
  k_stabilizer() : k_traverser("k_stabilizer") {}
  virtual ~k_stabilizer() {}
  virtual void execute(k_bucket*, string, unsigned, unsigned);
};
// }}}
// {{{ class k_stabilized
class k_stabilized : public k_traverser { public:
  k_stabilized(vector<Kademlia::NodeID> *v) :
    k_traverser("k_stabilized"), _v(v), _stabilized(true) {}

  virtual ~k_stabilized() {}
  virtual void execute(k_bucket*, string, unsigned, unsigned);
  bool stabilized() { return _stabilized; }

private:
  vector<Kademlia::NodeID> *_v;
  bool _stabilized;
};
// }}}
// {{{ class k_finder
class k_finder : public k_traverser { public:
  k_finder(Kademlia::NodeID n) : k_traverser("k_finder"), _n(n), _found(0) {}

  virtual ~k_finder() {}
  virtual void execute(k_bucket*, string, unsigned, unsigned);
  unsigned found() { return _found; }

private:
  Kademlia::NodeID _n;
  unsigned _found;
};
// }}}
// {{{ class k_dumper
class k_dumper : public k_traverser { public:
  k_dumper() : k_traverser("k_dumper") {}
  virtual ~k_dumper() {}
  virtual void execute(k_bucket*, string, unsigned, unsigned);

private:
};
// }}}
// {{{ class k_check
class k_check : public k_traverser { public:
  k_check() : k_traverser("k_check") {}
  virtual ~k_check() {}
  virtual void execute(k_bucket*, string, unsigned, unsigned);

private:
};
// }}}

#define KDEBUG(x) if(p2psim_verbose >= (x)) cout << Kademlia::debugcounter++ << "(" << now() << ")"<<ip() << " . " << Kademlia::printID(_id) << "(" << taskid() << ") "
#endif // __KADEMLIA_H
