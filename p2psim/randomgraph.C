#include "topology.h"
#include "protocol.h"
#include "protocolfactory.h"
#include "randomgraph.h"
#include "network.h"
#include "parse.h"
#include <iostream>
#include <fstream>
#include "p2psim.h"
#include <stdio.h>
using namespace std;

RandomGraph::RandomGraph(vector<string>*)
{
}

RandomGraph::~RandomGraph()
{
}

void
RandomGraph::parse(ifstream &ifs)
{
  string line;
  while(getline(ifs,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // nodeid and at least one protocol
    if(words.size() < 1) {
      cerr << "RandomGraph: provide nodeid per line" << endl;
      exit(1);
    }

    // node-id
    IPAddress ipaddr = atoi(words[0].c_str());
    if(!ipaddr){
      cerr << "node-id 0 is illegal." << endl;
      exit(1);
    }

    // add the node to the network
    Node *n = New Node(ipaddr);
    send(Network::Instance()->nodechan(), &n);

    // remember the new node in DVGraph's tables
    add_node(n->ip());
  }

  // Initalize links.
  int i, j;
  _links = (short *) malloc(sizeof(short) * _n * _n);
  assert(_links);
  for(i = 0; i < _n; i++)
    for(j = 0; j < _n; j++)
      links(i, j) = -1;

  // Generate some random links to/from each node.
  for(i = 0; i < _n; i++){
    for(j = 0; j < degree; j++){
      int k = random() % _n;          // pick a random node.
      int m = random() % maxlatency;  // pick a random link metric.
      links(i, k) = m;
      links(k, i) = m;
    }
  }
}
