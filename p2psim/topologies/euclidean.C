#include "p2psim/network.h"
#include "p2psim/parse.h"
#include "p2psim/topology.h"
#include "euclidean.h"
#include <cmath>
#include <iostream>
using namespace std;

Euclidean::Euclidean(vector<string>*)
{
}

Euclidean::~Euclidean()
{
}

Time
Euclidean::latency(IPAddress ip1, IPAddress ip2)
{
  Coord c1 = _nodes[ip1];
  Coord c2 = _nodes[ip2];

  return (Time) hypot(labs(c2.first - c1.first), labs(c2.second - c1.second));
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

    // nodeid, coordinates and at least one protocol
    if(words.size() < 2) {
      cerr << "provide nodeid and coordinates per line" << endl;
      continue;
    }

    // node-id
    IPAddress ipaddr = (IPAddress) strtoull(words[0].c_str(), NULL, 10);
    if(!ipaddr)
      cerr << "found node-id 0.  you're asking for trouble." << endl;

    // x,y coordinates
    vector<string> coords = split(words[1], ",");
    Coord c;
    c.first = atoi(coords[0].c_str());
    c.second = atoi(coords[1].c_str());

    // what kind of node?
    Node *n = New Node(ipaddr);

    // add the new node it to the topology
    if(_nodes.find(n->ip()) != _nodes.end())
      cerr << "warning: node " << ipaddr << " already added! (" <<words[0]<<")" << endl;
    _nodes[n->ip()] = c;

    // add the node to the network
    send(Network::Instance()->nodechan(), &n);
  }
}
