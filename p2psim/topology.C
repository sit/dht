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
  if(!in) {
    cerr << "no such file " << filename << endl;
    threadexitsall(0);
  }

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

  if(!top) {
    cerr << "the topology you specified is unknown" << endl;
    exit(-1);
  }

  // create the network
  Network::Instance(top);

  // leave the rest of the file to the specific topology
  top->parse(in);

  return top;
}
