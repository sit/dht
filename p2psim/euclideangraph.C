#include "topology.h"
#include "protocol.h"
#include "protocolfactory.h"
#include "nodefactory.h"
#include "euclideangraph.h"
#include "network.h"
#include "parse.h"
#include <iostream>
#include <fstream>
#include "p2psim.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
using namespace std;

EuclideanGraph::EuclideanGraph(vector<string>*)
{
}

EuclideanGraph::~EuclideanGraph()
{
}

void
EuclideanGraph::parse(ifstream &ifs)
{
  string line;
  while(getline(ifs,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // nodeid, nodetype, and at least one protocol
    if(words.size() < 4) {
      cerr << "EuclideanGraph: provide nodeid, coordinates, Node, and at least one protocol per line" << endl;
      exit(1);
    }

    // node-id
    IPAddress ipaddr = atoi(words[0].c_str());
    if(!ipaddr)
      cerr << "found node-id 0.  you're asking for trouble." << endl;

    // x,y coordinates
    vector<string> coords = split(words[1], ",");
    Coord c;
    c._x = atof(coords[0].c_str());
    c._y = atof(coords[1].c_str());

    // what kind of node?
    Node *n = NodeFactory::Instance()->create(words[2], ipaddr);

    // all the rest are protocols on this node
    for(unsigned int i=3; i<words.size(); i++)
      send(n->protchan(), &(words[i]));

    // add the node to the network
    send(Network::Instance()->nodechan(), &n);

    // remember the new node in DVGraph's tables
    add_node(n->ip());

    // remember the node's coordinates
    _coords.push_back(c);
  }

  // Initialize links.
  int i, j;
  _links = (short *) malloc(sizeof(short) * _n * _n);
  assert(_links);
  for(i = 0; i < _n; i++)
    for(j = 0; j < _n; j++)
      links(i, j) = -1;

  // Generate some random links to/from each node.
  for(i = 0; i < _n; i++){
    Coord c1 = _coords[i];
    for(j = 0; j < degree; j++){
      int k = random() % _n;          // pick a random node.
      Coord c2 = _coords[k];
      int m = (int) hypot(c2._x - c1._x, c2._y - c1._y);
      links(i, k) = m;
      links(k, i) = m;
    }
  }

  // Guess what the likely Vivaldi errors will be.
  // That is, the difference between direct Euclidean distance
  // and latency over the shortest path.
  double sum = 0;
  for(i = 0; i < _n; i++){
    Coord c1 = _coords[i];
    for(j = 0; j < _n; j++){
      Coord c2 = _coords[j];
      double d = hypot(c2._x - c1._x, c2._y - c1._y);
      latency_t lat = latency(_i2ip[i], _i2ip[j]);
      sum += fabs(d - lat);
    }
  }
  fprintf(stderr, "EuclideanGraph: typical Vivaldi error %.1f\n",
          sum / (_n * _n));
}

EuclideanGraph::Coord
EuclideanGraph::getcoords(IPAddress a)
{
  unsigned int i = _ip2i[a];
  assert(i >= 0 && i < _coords.size() && _i2ip[i] == a);
  return _coords[i];
}
