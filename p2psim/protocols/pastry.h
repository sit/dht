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

#ifndef __PASTRY_H
#define __PASTRY_H

#include "p2psim/dhtprotocol.h"

class Pastry : public DHTProtocol {
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
