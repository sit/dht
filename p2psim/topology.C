#include "topology.h"
#include "topologyfactory.h"
#include "network.h"
#include <iostream>
#include <fstream>

using namespace std;


Topology::Topology()
{
}

Topology::~Topology()
{
}


Topology*
Topology::parse(char *filename)
{
  ifstream in(filename);

  string w1, w2;
  in >> w1 >> w2;

  // read topology string
  assert(w1 == "topology");
  Topology *top = TopologyFactory::create(w2);
  assert(top);

  // create the network
  Network::Instance(top);

  // leave the rest of the file to the specific topology
  top->parse(in);

  return top;
}
