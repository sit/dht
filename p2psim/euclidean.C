#include "topology.h"
#include "protocol.h"
#include "protocolfactory.h"
#include "euclidean.h"
#include "network.h"
#include "parse.h"
#include <cmath>
#include <iostream>
#include <fstream>
#include <vector>
#include "p2psim.h"
using namespace std;

Euclidean::Euclidean()
{
}

Euclidean::~Euclidean()
{
}

latency_t
Euclidean::latency(Node *n1, Node *n2)
{
  Coord c1 = _nodes[n1->ip()];
  Coord c2 = _nodes[n2->ip()];

  return (latency_t) hypot(labs(c2.first - c1.first), labs(c2.second - c1.second));
}


void
Euclidean::parse(ifstream &ifs)
{
  string line;
  while(getline(ifs,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // node-id
    IPAddress ipaddr = atoi(words[0].c_str());
    if(!ipaddr)
      cerr << "found node-id 0.  you're asking for trouble." << endl;
    Node *n = new Node(ipaddr);

    // x,y coordinates
    vector<string> coords = split(words[1], ",");
    Coord c;
    c.first = atoi(coords[0].c_str());
    c.second = atoi(coords[1].c_str());

    // all the rest are protocols on this node
    for(unsigned int i=2; i<words.size(); i++) {
      // run the specified protocol on the node
      Protocol *p = ProtocolFactory::Instance()->create(words[i], n);
      send(n->protchan(), &(words[i]));
      assert(p);
    }

    // add the new node it to the topology
    if(_nodes.find(n->ip()) != _nodes.end())
      cerr << "warning: node " << ipaddr << " already added!" << endl;
    _nodes[n->ip()] = c;

    // add the node to the network
    send(Network::Instance()->nodechan(), &n);
  }
}
