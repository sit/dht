#ifndef __KADEMLIA_H
#define __KADEMLIA_H

#include "protocol.h"
#include "node.h"
#include <map>
#include <iostream>
using namespace std;

#define KDEBUG(x) DEBUG(x) << printbits(_id) << " "


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
  void dumpfingers() { _fingers.dump(_id); };
  static NodeID distance(NodeID, NodeID);

  bool stabilized(vector<NodeID>);
  void dump() { cout << "*** DUMP for " << printbits(_id) << " ***" <<endl;  _fingers.dump(_id); };
  NodeID id () { return _id;}

  // bit twiddling utility functions
  static NodeID flipbitandmaskright(NodeID, unsigned);
  static NodeID maskright(NodeID, unsigned);

private:
  NodeID _id;


  // join
  struct join_args {
    NodeID id;
    IPAddress ip;
  };
  struct join_result {
    int ok;
  };
  void do_join(void *args, void *result);
  void merge_into_ftable(NodeID id, IPAddress ip);


  // lookup
  struct lookup_args {
    NodeID id;
    IPAddress ip;

    NodeID key;
  };
  struct lookup_result {
    NodeID id;
    IPAddress ip;
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
    fingers_t *fingers;
  };
  void do_transfer(void *args, void *result);
                                                                                  
  // finger table
  class fingers_t { public:
    fingers_t(NodeID id) : _id(id) {};
    void set(unsigned i, NodeID id, IPAddress ip) {
      _ft[i].id = id;
      _ft[i].valid = true;
      _ft[i].retries = 0;
      _id2ip[id] = ip;
      cout << "set " << i << endl;
      cout << "_id " << printbits(_id) << endl;
      cout << " id " << printbits(id) << endl;
      cout << " fp " << printbits(Kademlia::flipbitandmaskright(_id, i)) << endl;
      cout << " mr " << printbits(Kademlia::maskright(id, i)) << endl;
      assert(Kademlia::flipbitandmaskright(_id, i) == Kademlia::maskright(id, i));
    }

    void unset(unsigned i) { _ft[i].valid = false; }
    void dump(NodeID myid) {
      string s;
      s = "i\tmyid permu\tnearest node\n";
      for(unsigned i=0; i<Kademlia::idsize; i++)
        cout << i << "\t" << Kademlia::printbits(myid ^ (1<<i)) << "\t" << Kademlia::printbits(get_id(i)) << endl;
    }

    bool valid(unsigned i)          { return _ft[i].valid; }
    unsigned retries(unsigned i)    { return _ft[i].retries; }
    IPAddress get_ip(unsigned i)    { return _id2ip[_ft[i].id]; }
    NodeID get_id(unsigned i)       { return _ft[i].id; }
    IPAddress get_ipbyid(NodeID id) { return _id2ip[id]; }

  private:
    map<NodeID, IPAddress> _id2ip;

    class peer_t { public:
      bool valid;
      unsigned retries;
      NodeID id;
      peer_t() { valid = false; retries = 0; id = 0; };
    } _ft[8*sizeof(NodeID)];

    NodeID _id;
  } _fingers;

  // this is what we're here for: being a NodeID -> value hashtable
  map<NodeID, Value> _values;


  static NodeID _rightmasks[];
};

#endif // __KADEMLIA_H
