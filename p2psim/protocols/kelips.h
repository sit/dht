#ifndef __KELIPS_H
#define __KELIPS_H

// Differences from Kelips as described in IPTPS 2003 paper:
// I look up IDs, not files.
// Not clear who you gossip to across groups, and what you say.
// When/how do you learn RTT to a new node? So you know whether to
//   keep it in your contacts list.
// How often does a node generate a new heartbeat value?
// When does a node send Info for itself? random? every gossip?
// For how many rounds do they gossip a new item? (1 seems to work best)
// Do existing items with new heartbeats count as new for gossip?
// How big are the rations mentions in 2.1? In the eval.
//   For group vs contact, and also new vs old.
// Pull at join and then all push often leaves isolated nodes,
//   since maybe nobody pushes to me. So what happens at join?
// ***
// What heartbeat timeout values?
// The contacts tend to time out a lot...
// Replacing contacts with newer heartbeat helps convergence a lot.
//   As well as reducing contact timeout dramatically.
// Are lookups iterative or recursive? Who controls various retries?

// Does it stabilize after the expected number of rounds?
// Gossip w/o favoring new nodes (nnodes: avg median):
//   100: 10 10
//   400: 27 27
// With gossiping new nodes first for 1 round:
//   100: 10 11
//   400: 25 29
// Replacing contacts with newer heartbeat, _item_rounds = 1:
//   100:  6  5
//   400: 13 14
// Oy, _item_rounds=0 beats this by a lot.

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

  // send gossip every two seconds, to 3 targets in group,
  // and three contacts outside the group. Section 4.
  static const int _round_interval = 2000;
  static const u_int _group_targets = 3;
  static const u_int _contact_targets = 3;

  // Items per gossip packet. Sections 2 and 4 imply 272/40 = 8 total.
  // XXX but doesn't specify the group vs contact breakdown.
  static const u_int _group_ration = 4;
  static const u_int _contact_ration = 2;

  // number of contacts to remember for each foreign group. Section 2.
  static const int _n_contacts = 2;

  // how many times to gossip a new item. XXX not specified in paper.
  static const int _item_rounds = 1;

  // hearbeat timeouts. XXX not specified in paper.
  static const int _group_timeout = (_round_interval * 25);
  static const int _contact_timeout = (_round_interval * 50);

  int _k; // number of affinity groups, should be sqrt(n)

  bool _started; // are our timers ticking?
  bool _live;    // are we joined but not crashed?
  int _rounds;  // how many?
  bool _stable;

  // Information about one other node.
  class Info {
  public:
    IPAddress _ip;
    Time _heartbeat; // when _ip last spoke to anyone.
    int _rounds;     // how many rounds to send for.
    Info(IPAddress ip, Time hb) { _ip = ip; _heartbeat = hb; _rounds = 0; }
    Info() { _ip = 0; _heartbeat = 0; _rounds = 0; }
  };

  // Set of nodes that this node knows about.
  // This is the paper's Affinity Group View and Contacts.
  map<IPAddress, Info *> _info;

  void gotinfo(Info i);
  int id2group(ID id) { return(id % _k); }
  int ip2group(IPAddress xip) { return(id2group(ip2id(xip))); }
  ID ip2id(IPAddress xip) {
    return xip;
    // return ConsistentHash::ip2chid(xip);
  }
  ID id() { return ip2id(ip()); }
  int group() { return ip2group(ip()); }
  void gossip(void *);
  void handle_gossip(vector<Info> *, void *);
  void purge(void *);
  vector<IPAddress> all();
  vector<IPAddress> grouplist(int g);
  vector<IPAddress> notgrouplist(int g);
  void check(bool doprint);
  void handle_join(IPAddress *caller, vector<Info> *ret);
  vector<Info> gossip_msg(int g);
  vector<IPAddress> randomize(vector<IPAddress> a);
  vector<IPAddress> newold(vector<IPAddress> a, bool xnew);
  void newold_msg(vector<Info> &msg, vector<IPAddress> l, u_int ration);
  void handle_lookup_final(ID *kp, bool *done);
  void handle_lookup1(ID *kp, IPAddress *res);
  bool lookup1(ID key, vector<IPAddress> &);
  bool lookup2(ID key, vector<IPAddress> &);
  bool lookup_loop(ID key, vector<IPAddress> &);
  void handle_lookup2(ID *kp, IPAddress *res);
  IPAddress find_by_id(ID key);
};

#endif
