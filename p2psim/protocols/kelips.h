#ifndef __KELIPS_H
#define __KELIPS_H

// Differences from Kelips as described in IPTPS 2003 paper:
// I look up IDs, not files.
// Not clear who you gossip to across groups, and what you say.
// On startup, does well-known node maintain lots of information about
//   other groups? If not, how do 
// When/how do you learn RTT to a new node? So you know whether to
//   keep it in your contacts list.

#include "p2psim/p2protocol.h"
#include "consistenthash.h"
#include "p2psim/p2psim.h"
#include "p2psim/condvar.h"
#include <map.h>

class Kelips : public P2Protocol {
public:
  Kelips(Node *n, Args a);
  virtual ~Kelips();
  string proto_name() { return "Kelips"; }

  // Functions callable from events file.
  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void lookup(Args*);
  virtual void insert(Args*);

 private:
  typedef ConsistentHash::CHID ID;

  int _k; // number of affinity groups, should be sqrt(n)

  // Information about one other node.
  class Info {
  public:
    Info(IPAddress ip) { _ip = ip; }
    Info() { }
    IPAddress _ip;
  };

  // Set of nodes that this node knows about.
  // This is the paper's Affinity Group View and Contacts.
  map<IPAddress, Info> _info;

  void gotinfo(Info i);
  int ip2group(IPAddress xip) { return(ip2id(xip) % _k); }
  ID ip2id(IPAddress xip) {  return ConsistentHash::ip2chid(xip); }
  ID id() { return ip2id(ip()); }
};

#endif
