#ifndef __TOPOLOGY_H
#define __TOPOLOGY_H

#include "p2psim.h"
#include "node.h"
#include <fstream>
using namespace std;

// abstract super class of a topology
class Topology {
public:
  // will create the appropriate topology object
  static Topology* parse(char *);
  virtual void parse(ifstream&) = 0;

  virtual latency_t latency(Node*, Node*) = 0;

protected:
  Topology();
  ~Topology();
};

#endif //  __TOPOLOGY_H
