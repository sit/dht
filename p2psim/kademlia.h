#ifndef __KADEMLIA_H
#define __KADEMLIA_H

#include "protocol.h"
#include "node.h"
#include <map>
#include <iostream>
using namespace std;

extern unsigned kdebugcounter;
class k_bucket;

class Kademlia : public Protocol {
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

  bool stabilized(vector<NodeID>, k_bucket* = 0, unsigned = 0);
  void dump();
  NodeID id () { return _id;}

  // bit twiddling utility functions
  static NodeID flipbitandmaskright(NodeID, unsigned);
  static NodeID maskright(NodeID, unsigned);
  static unsigned getbit(NodeID, unsigned);
  static unsigned k()   { return _k; }

private:
  NodeID _id;
  static unsigned _k;


  // join
  struct join_args {
    NodeID id;
    IPAddress ip;
  };
  struct join_result {
    int ok;
  };
  void do_join(void *args, void *result);


  // lookup
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

  void do_lookup(void *args, void *result);


  // insert
  struct insert_args {
    NodeID id;
    IPAddress ip;

    NodeID key;
    Value val;
  };
  struct insert_result {
  };
  void do_insert(void *args, void *result);


  // transfer
  class fingers_t;
  struct transfer_args {
    NodeID id;
    IPAddress ip;
  };
  struct transfer_result {
    map<NodeID, Value> values;
  };
  void do_transfer(void *args, void *result);


  k_bucket *_root;



  /*
  class bucket_t { public:
    bucket_t(NodeID id) : _id(id) {};

    void insert(NodeID id, IPAddress ip) {
      if(_root.insert(id))
        _id2ip[id] = ip;
    }

      *
      _ft[i].id = id;
      _ft[i].valid = true;
      _ft[i].retries = 0;
      KDEBUG(5) << "set " << i << endl;
      KDEBUG(5) << "_id " << printbits(_id) << endl;
      KDEBUG(5) << " id " << printbits(id) << endl;
      KDEBUG(5) << " fp " << printbits(Kademlia::flipbitandmaskright(_id, i)) << endl;
      KDEBUG(5) << " mr " << printbits(Kademlia::maskright(id, i)) << endl;
      assert(Kademlia::flipbitandmaskright(_id, i) == Kademlia::maskright(id, i));
      /

    void unset(unsigned i) { _ft[i].valid = false; }
    void dump(NodeID myid) {
      for(unsigned i=0; i<Kademlia::idsize; i++) {
        cout << i << "\t" << Kademlia::printbits(myid ^ (1<<i)) << "\t";
        if(valid(i))
          cout << Kademlia::printbits(get_id(i));
        else
          cout << "[invalid]";
        cout << endl;
      }
    }


    bool valid(unsigned i)          { return _ft[i].valid; }
    unsigned retries(unsigned i)    { return _ft[i].retries; }
    IPAddress get_ip(unsigned i)    { return _id2ip[_ft[i].id]; }
    NodeID get_id(unsigned i)       { return _ft[i].id; }
    IPAddress get_ipbyid(NodeID id) { return _id2ip[id]; }

  private:
    k_bucket _root;
    map<NodeID, IPAddress> _id2ip;
    NodeID _id;
  } _buckets;
  */

  // this is what we're here for: being a NodeID -> value hashtable
  map<NodeID, Value> _values;

  void reschedule_stabilizer(void*);
  void stabilize(k_bucket*, NodeID = 0);

  void _dump(k_bucket*, string, unsigned);


  static NodeID _rightmasks[];
};


//
//
// K-BUCKETS
//
//
// one entry in k_bucket's _nodes vector
class peer_t {
public:
  peer_t(Kademlia::NodeID xid, IPAddress xip, Time t) : retries(0), id(xid), ip(xip), lastts(t) {}
  peer_t(const peer_t &p) : retries(0), id(p.id), ip(p.ip), lastts(p.lastts) {}
  unsigned retries;
  Kademlia::NodeID id;
  IPAddress ip;
  Time lastts;
};


// a k-bucket
class k_bucket {
public:
  k_bucket(Kademlia::NodeID);
  ~k_bucket();

  vector<peer_t*> _nodes;
  k_bucket* _child[2]; // subtree

  unsigned insert(Kademlia::NodeID node, IPAddress ip, Kademlia::NodeID prefix = 0, unsigned depth = 0, k_bucket *root = 0);
  void dump(k_bucket *, Kademlia::NodeID = 0);

private:
  static unsigned _k;
  Kademlia::NodeID _id;
};


#define KDEBUG(x) DEBUG(x) << kdebugcounter++ << ". " << Kademlia::printbits(_id) << " "

#endif // __KADEMLIA_H
