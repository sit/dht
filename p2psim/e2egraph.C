#include "topology.h"
#include "protocol.h"
#include "protocolfactory.h"
#include "e2egraph.h"
#include "network.h"
#include "parse.h"
#include <cmath>
#include <iostream>
#include <fstream>
#include <vector>
#include "p2psim.h"

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

latency_t
E2EGraph::latency(IPAddress ip1, IPAddress ip2)
{
  //we can not use random ip address for e2egraph now
  assert(ip1 > 0 && ip1 <= _num);
  assert(ip2 > 0 && ip2 <= _num);
  return (latency_t) _pairwise[ip1-1][ip2-1];
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

    if (words.size() >= 3) {

      IPAddress ipaddr = atoi(words[0].c_str());
      assert(ipaddr > 0 && ipaddr <= _num);

      // what kind of node?
      Node *n = new Node(ipaddr);

      // all the rest are protocols on this node
      for(unsigned int i=2; i<words.size(); i++)
	send(n->protchan(), &(words[i]));

      // add the node to the network
      send(Network::Instance()->nodechan(), &n);

    } else {

      // x,y coordinates
      vector<string> pair = split(words[0], ",");
      IPAddress ip1 = atoi(pair[0].c_str());
      IPAddress ip2 = atoi(pair[1].c_str());
      assert(ip1>0 && ip1 <= _num);
      assert(ip2>0 && ip2 <= _num);

      int lat = atoi(words[1].c_str());
      if (lat < 0) {
	lat = 0;
      }
      // latency between node1 and node2
      _pairwise[ip1 -1][ip2 -1] = (latency_t) lat;
    }
  }
}
