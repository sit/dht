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

#include "p2psim/topology.h"
#include "p2psim/network.h"
#include "protocols/protocolfactory.h"
#include "e2egraph.h"
#include <iostream>

using namespace std;

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
E2EGraph::latency(IPAddress ip1x, IPAddress ip2x, bool reply)
{
  IPAddress ip1 = Network::Instance()->first_ip(ip1x);
  IPAddress ip2 = Network::Instance()->first_ip(ip2x);

  if(!(ip1 > 0 && ip1 <= _num) ||
    (!(ip2 > 0 && ip2 <= _num)))
  {
    cout << "ip1 = " << ip1 << ", ip2 = " << ip2 << endl;
  }

  //we can not use random ip address for e2egraph now
  assert(ip1 > 0 && ip1 <= _num);
  assert(ip2 > 0 && ip2 <= _num);
  int t;
  if (ip1 < ip2)  {
    t =  _pairwise[ip1-1][ip2-1];
  } else if (ip1 > ip2) {
    t = _pairwise[ip2-1][ip1-1];
  } else{
    t = 0;
  }
  if (t < 0) {
    t = _med_lat; //a missing measurement, subsitute with median latency
  }else if ((t == 0) &&(ip1!=ip2)) {
    t = 1;
  }
  return (Time)t;
}

bool
E2EGraph::valid_latency (IPAddress ip1x, IPAddress ip2x)
{
  IPAddress ip1 = Network::Instance()->first_ip(ip1x);
  IPAddress ip2 = Network::Instance()->first_ip(ip2x);

  assert(ip1 > 0 && ip1 <= _num);
  assert(ip2 > 0 && ip2 <= _num);
  int t;
  if (ip1 < ip2)  {
    t =  _pairwise[ip1-1][ip2-1];
  } else if (ip1 > ip2) {
    t = _pairwise[ip2-1][ip1-1];
  } else {
    t = 0;
  }

  return (t > 0);
}

void
E2EGraph::parse(ifstream &ifs)
{
  string line;
  vector<Time> tmp;

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
      Node *p;
      if (words.size()<=2) {
	p = ProtocolFactory::Instance()->create(ipaddr);
      }else{
	p = ProtocolFactory::Instance()->create(ipaddr,words[2].c_str());
      }

      // add the node to the network
      send(Network::Instance()->nodechan(), &p);


    } else {

      vector<string> pair = split(words[0], ",");
      IPAddress ip1 = atoi(pair[0].c_str());
      IPAddress ip2 = atoi(pair[1].c_str());
      assert(ip1>0 && ip1 <= _num);
      assert(ip2>0 && ip2 <= _num);

      int lat = atoi(words[1].c_str());

      // latency between node1 and node2
      _pairwise[ip1 -1][ip2 -1] = (Time) lat;
      if (lat > 0) 
	tmp.push_back((Time)lat);
    }
  }
  sort(tmp.begin(),tmp.end());
  _med_lat = tmp[tmp.size()/2];

}

#include "p2psim/bighashmap.cc"
