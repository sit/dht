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
 */

#include "p2psim/topology.h"
#include "p2psim/network.h"
#include "p2psim/parse.h"
#include "e2egraph.h"

E2EGraph::E2EGraph(vector<string> *v)
{
  _num = atoi((*v)[0].c_str());
  assert(_num > 0);

  _pairwise.resize(_num);
  for (unsigned int i = 0; i < _num; i++) 
    _pairwise[i].resize(_num);
}

E2EGraph::~E2EGraph()
{
}

Time
E2EGraph::latency(IPAddress ip1, IPAddress ip2)
{
  //we can not use random ip address for e2egraph now
  assert(ip1 > 0 && ip1 <= _num);
  assert(ip2 > 0 && ip2 <= _num);
  if (ip1 < ip2)  {
    assert(_pairwise[ip1-1][ip2-1] > 0 && _pairwise[ip1-1][ip2-1] <= 1000000);
    return (Time) _pairwise[ip1-1][ip2-1];
  } else if (ip1 > ip2) {
    assert(_pairwise[ip2-1][ip1-1] > 0 && _pairwise[ip2-1][ip1-1] <= 1000000);
    return (Time) _pairwise[ip2-1][ip1-1];
  } else{
    return 0;
  }
}


void
E2EGraph::parse(ifstream &ifs)
{
  string line;

  while(getline(ifs,line)) {
    vector<string> words = split(line);
    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // XXX Thomer:  The else clause also seems
    // to expect 2 words per line?
    if (words[0] == "node") {
      IPAddress ipaddr = atoi(words[1].c_str());
      assert(ipaddr > 0 && ipaddr <= _num);

      // what kind of node?
      Node *n = New Node(ipaddr);

      assert(!Network::Instance()->getnode(ipaddr));

      // add the node to the network
      send(Network::Instance()->nodechan(), &n);

    } else {

      vector<string> pair = split(words[0], ",");
      IPAddress ip1 = atoi(pair[0].c_str());
      IPAddress ip2 = atoi(pair[1].c_str());
      assert(ip1>0 && ip1 <= _num);
      assert(ip2>0 && ip2 <= _num);

      int lat = atoi(words[1].c_str());
      if (lat < 0) {
	lat = 100000; //XXX missing data is treated as infinity, this is sketchy
      }
      // latency between node1 and node2
      _pairwise[ip1 -1][ip2 -1] = (Time) lat;
    }
  }
}
