/*
 * Copyright (c) 2003-2005 Jinyang Li
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

#ifndef __DVGRAPH_H
#define __DVGRAPH_H

// Abstract class that helps you write Topologies that
// need to compute multi-hop routes. You sub-class
// DVGraph, indicate which nodes have direct links to each
// other (and the link latencies), and DVGraph will compute
// the paths between all nodes and the path latencies.
// So DVGraph implements latency() for you.

#include "p2psim/topology.h"

class DVGraph : public Topology {
public:
  DVGraph();
  ~DVGraph();
  
  Time latency(IPAddress a, IPAddress b, bool reply = false);

 protected:
  // subclass must fill in these values by calling add_node()
  int _n; // # of nodes
  vector<IPAddress> _i2ip;
  hash_map<IPAddress, int> _ip2i;
  short *_links;   // 2-d matrix of link delays, -1 for none.

  void add_node(IPAddress a);
  short& links(int i, int j) { return _links[i*_n + j]; }
  
 private:
  int _initialized;

  // per-node forwarding tables
  struct Entry {
    int _next;
    int _metric;
    int _hops;
  };
  Entry *_routes;
  Entry& routes(int i, int j) { return _routes[i*_n + j]; }

  void dv();
};

#endif //  __DVGRAPH_H
