#include "topology.h"
#include "topologyfactory.h"
#include "network.h"
#include "parse.h"
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

  string line;
  Topology *top = 0;
  while(getline(in,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // read topology string
    if(words[0] != "topology") {
      cerr << "first line in topology file should be ``topology [T]''" << endl;
      exit(-1);
    }

    top = TopologyFactory::create(words[1]);
    break;
  }
  assert(top);

  // create the network
  Network::Instance(top);

  // leave the rest of the file to the specific topology
  top->parse(in);

  return top;
}
