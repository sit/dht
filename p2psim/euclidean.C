#include "topology.h"
#include "protocol.h"
#include "protocolfactory.h"
#include "nodefactory.h"
#include "euclidean.h"
#include "network.h"
#include "parse.h"
#include <cmath>
#include <iostream>
#include <fstream>
#include <vector>
#include "p2psim.h"
using namespace std;

Euclidean::Euclidean(vector<string>*)
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

    // nodeid, coordinates, nodetype, and at least one protocol
    if(words.size() < 4) {
      cerr << "provide nodeid, coordinates, Node, and at least one protocol per line" << endl;
      continue;
    }

    // node-id
    IPAddress ipaddr = atoi(words[0].c_str());
    if(!ipaddr)
      cerr << "found node-id 0.  you're asking for trouble." << endl;

    // x,y coordinates
    vector<string> coords = split(words[1], ",");
    Coord c;
    c.first = atoi(coords[0].c_str());
    c.second = atoi(coords[1].c_str());

    // what kind of node?
    Node *n = NodeFactory::Instance()->create(words[2], ipaddr);

    // add the new node it to the topology
    if(_nodes.find(n->ip()) != _nodes.end())
      cerr << "warning: node " << ipaddr << " already added!" << endl;
    _nodes[n->ip()] = c;

    // all the rest are protocols on this node
    for(unsigned int i=3; i<words.size(); i++)
      send(n->protchan(), &(words[i]));

    // add the node to the network
    send(Network::Instance()->nodechan(), &n);
  }
}
