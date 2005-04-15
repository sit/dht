/*
* Copyright (c) 2003-2005 Steve Gerding
*                    Jeremy Stribling
*					Massachusetts Institute of Technology
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
#include "p2psim/parse.h"
#include "protocols/protocolfactory.h"

#include "e2elinkfailgraph.h"
#include <math.h>

E2ELinkFailGraph::E2ELinkFailGraph(vector<string> *v)
{
  // make sure v has size of 2
  assert(v->size() == 2);
  
  // get number of nodes
  _num_nodes = atoi((*v)[0].c_str());
  assert(_num_nodes > 0);
  
  // get number of time samples
  _num_samples = atoi((*v)[1].c_str());
  assert(_num_samples > 0);
  
  // resize latency vector to [_num_nodes][_num_nodes] and link failure vector to [_num_samples]
  _pairwise.resize(_num_nodes);
  _link_working.resize(_num_nodes);
  for (unsigned int i = 0; i < _num_nodes; i++){
    _pairwise[i].resize(_num_nodes);
    _link_working[i].resize(_num_nodes);
    for (unsigned int j = 0; j < _num_nodes; j++){
      _link_working[i][j].resize(_num_samples);
    }
  }
}

E2ELinkFailGraph::~E2ELinkFailGraph()
{
}

Time
E2ELinkFailGraph::latency(IPAddress ip1, IPAddress ip2, bool reply)
{
  // we can not use random ip address for e2egraph now
  assert(ip1 > 0 && ip1 <= _num_nodes);
  assert(ip2 > 0 && ip2 <= _num_nodes);
  
  // if nodes are the same, return 0
  if(ip1 == ip2){
    return 0;
  }
  
  /*
  // if it's a reply, we switch these around because all we know are
  // half rtts
  if( reply ) {
    IPAddress tmp = ip1;
    ip1 = ip2;
    ip2 = tmp;
  }
  */

  // decide what time it is (assume 15 minute data intervals)
  int time_index = now() / 900000;
  
  // make sure latency exists and return it
  assert(_pairwise[ip1 - 1][ip2 - 1] >= 0);
  if(_link_working[ip1 - 1][ip2 - 1][time_index]){
    return (Time) _pairwise[ip1 - 1][ip2 - 1];
  }else{
    return 100000;
  }
}


void
E2ELinkFailGraph::parse(ifstream &ifs)
{
  string line;
  
  while(getline(ifs,line)) {
    
    // get fields of current line
    vector<string> words = split(line);
    
    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;
    
    // if line is a node declaration
    if (words[0] == "node") {
      
      // read ip address
      IPAddress ipaddr = atoi(words[1].c_str());
      assert(ipaddr > 0 && ipaddr <= _num_nodes);
      
      // what kind of node?
      Node *p = ProtocolFactory::Instance()->create(ipaddr);
      
      // make sure node doesn't exist already
      assert(!Network::Instance()->getnode(ipaddr));
      
      // add the node to the network
      send(Network::Instance()->nodechan(), &p);
      
      // otherwise, line is a link record
    } else {
      
      // get ip addresses
      vector<string> pair = split(words[0], ",");
      IPAddress ip1 = atoi(pair[0].c_str());
      IPAddress ip2 = atoi(pair[1].c_str());
      assert(ip1 > 0 && ip1 <= _num_nodes);
      assert(ip2 > 0 && ip2 <= _num_nodes);
      
      // read and store latency
      _pairwise[ip1 - 1][ip2 - 1] = 
	(Time) ceil(atof(words[1].c_str())/2);
      
      // read and store link status entries
      for(unsigned int i = 0; i < _num_samples; i++){
	_link_working[ip1 - 1][ip2 - 1][i] = 
	  (bool) atoi(words[i + 2].c_str());
      }
    }
  }
}
