#ifndef __KADEMLIA_H
#define __KADEMLIA_H

#include "protocol.h"
#include "node.h"
#include <map>
#include <iostream>
using namespace std;

class Kademlia : public Protocol {
public:
  typedef long long NodeID;
  static const unsigned idsize = 8*sizeof(NodeID);

  Kademlia(Node*);
  ~Kademlia();

  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void insert(Args*);
  virtual void lookup(Args*);

  void handle_join(void *args, void *result);
  void handle_join(NodeID id, IPAddress ip);

private:
  static NodeID distance(NodeID, NodeID);
  static string printbits(NodeID);

  NodeID _id;


  struct join_args {
    NodeID id;
    IPAddress ip;
  };
  struct join_result {
    int ok;
  };
                                                                                  

  // finger table
  class fingers_t { public:
    fingers_t() {};
    void set(unsigned i, NodeID id, IPAddress ip) {
      _ft[i].id = id;
      _ft[i].valid = true;
      _ft[i].retries = 0;
      _id2ip[id] = ip;
    }
    void unset(unsigned i)          { _ft[i].valid = false; }
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
  } _fingers;

};

#endif // __KADEMLIA_H
