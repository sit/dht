#ifndef __PASTRY_H
#define __PASTRY_H

#include "protocol.h"
#include "node.h"
#include <vector>
#include <set>
using namespace std;

class Pastry : public Protocol {
public:
  const unsigned idlength;
  typedef long long NodeID;

  Pastry(Node*);
  ~Pastry();
  string proto_name() { return "Pastry"; }

  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void insert(Args*);
  virtual void lookup(Args*);

private:
  NodeID _id;
  const unsigned _b; // config. parameter b from paper, base of NodeID
  const unsigned _L; // config. parameter L from paper, 
  const unsigned _M; // config. parameter M from paper, size of neighborhood

  void route(NodeID*, void*);
  unsigned shared_prefix_len(NodeID, NodeID);
  unsigned get_digit(NodeID, unsigned);


  struct join_msg_args {
    NodeID id;    // id of the guy who's joining
    IPAddress ip; // ip address of the guy who's joining
  };
  struct join_msg_ret {
    NodeID id;
    IPAddress ip;
  };
  void do_join(join_msg_args*, join_msg_ret*);


  //
  // ROUTING TABLE
  //
  // routing table entry
  class RTEntry : public pair<NodeID, IPAddress>  { public:
    RTEntry(pair<NodeID, IPAddress> p) { first = p.first; second = p.second; }
    bool operator<= (const NodeID n) const { return first <= n; }
    bool operator< (const NodeID n) const { return first < n; }
    bool operator>= (const NodeID n) const { return first >= n; }
    bool operator> (const NodeID n) const { return first > n; }
  };

  // single row in a routing table
  typedef vector<RTEntry> RTRow;

  // whole routing table.  is array since
  vector<RTRow> _rtable;

  //
  // NEIGHBORHOOD SET
  //
  typedef set<RTEntry> ES;
  ES _neighborhood;

  //
  // LEAF SET
  //
  ES _lleafset; // lower half of leaf set
  ES _hleafset; // higher half of leaf set

  // finds IP address such that (D - RTEntry) is minimal
  class RTEntrySmallestDiff { public:
    RTEntrySmallestDiff(NodeID D) : diff(-1), _D(D) {}
    public:
      void operator()(const Pastry::RTEntry &rt) {
        NodeID newdiff;
        if((newdiff = (_D - rt.first)) < diff) {
          diff = newdiff;
          ip = rt.second;
        }
      }
      IPAddress ip;
      NodeID diff;
    private:
      NodeID _D;
  };

  // finds node such that shl(RTEntry, D) >= l
  // and |T - D| < |A - D|
  /*
  class RTEntrySmallestDiff { public:
    RTEntrySmallestDiff(NodeID D, NodeID A) : _D(D), _A(A) {}
    public:
      void operator()(const Pastry::RTEntry &rt) {
        if(
          diff = newdiff;
          ip = rt.second;
        }
      }
      IPAddress ip;
    private:
      NodeID _D;
      NodeID _A;
  };
  */
};

#endif // __PASTRY_H
