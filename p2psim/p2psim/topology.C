#include "topologies/topologyfactory.h"
#include "failuremodels/failuremodelfactory.h"
#include "network.h"
#include "parse.h"
#include <iostream>
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
  extern bool with_failure_model;

  ifstream in(filename);
  if(!in) {
    cerr << "no such file " << filename << endl;
    threadexitsall(0);
  }

  string line;
  Topology *top = 0;
  FailureModel *failure_model = 0;
  while(getline(in,line)) {
    vector<string> words = split(line);

    // break on first empty line
    if(words.empty())
      break;

    // skip empty lines and commented lines
    if(words[0][0] == '#')
      continue;

    // read topology string
    if(words[0] == "topology") {
      words.erase(words.begin());
      string topology = words[0];
      words.erase(words.begin());
      top = TopologyFactory::create(topology, &words);
    } else if(words[0] == "failure_model") {
      words.erase(words.begin());
      string fm = words[0];
      words.erase(words.begin());
      failure_model = FailureModelFactory::create(fm, &words);
      if(!with_failure_model)
        cerr << "warning: -f flag but found failure_model keyword. ignoring failure_model!" << filename << endl;
    } else {
      cerr << "first line in topology file should be ``topology [T]'' or ``failure_model [F]" << endl;
      exit(-1);
    }
  }

  if(!top) {
    cerr << "the topology you specified is unknown" << endl;
    exit(-1);
  }

  if(!failure_model) {
    cerr << "the failure_model you specified is unknown" << endl;
    exit(-1);
  }

  // create the network
  Network::Instance(top, failure_model);

  // leave the rest of the file to the specific topology
  top->parse(in);

  return top;
}
