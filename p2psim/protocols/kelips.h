#ifndef __KELIPS_H
#define __KELIPS_H

// Differences from Kelips as described in IPTPS 2003 paper:
// I look up IDs, not files.
// Not clear who you gossip to across groups, and what you say.
// On startup, does well-known node maintain lots of information about
//   other groups? If not, how do 
// When/how do you learn RTT to a new node? So you know whether to
//   keep it in your contacts list.
// Do the gossip messages convey how stale the forwarded info is?
// How do we decide a node is so stale as to be dead?

#include "p2psim/p2protocol.h"
#include "consistenthash.h"
#include "p2psim/p2psim.h"
#include "p2psim/condvar.h"
#include <map>
#include <vector>

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

  static const int _gossip_interval = 1000;
  static const int _gossip_items = 5;
  static const int _purge_interval = 5000;
  static const int _n_contacts = 2;

  int _k; // number of affinity groups, should be sqrt(n)

  // Information about one other node.
  class Info {
  public:
    IPAddress _ip;
    Info(IPAddress ip) { _ip = ip; }
    Info() { }
  };

  // Set of nodes that this node knows about.
  // This is the paper's Affinity Group View and Contacts.
  map<IPAddress, Info *> _info;

  void gotinfo(Info i);
  int ip2group(IPAddress xip) { return(ip2id(xip) % _k); }
  ID ip2id(IPAddress xip) {
    return xip;
    // return ConsistentHash::ip2chid(xip);
  }
  ID id() { return ip2id(ip()); }
  int group() { return ip2group(ip()); }
  IPAddress random_peer();
  void gossip(void *);
  void handle_gossip(vector<Info> *, void *);
  void purge(void *);
  vector<IPAddress> all();
  void check();
  void handle_join(IPAddress *caller, vector<Info> *ret);
};

#endif
