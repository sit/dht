#ifndef __TOPOLOGY_H
#define __TOPOLOGY_H

// abstract super class of a topology
#include <fstream>
#include "p2psim.h"
using namespace std;

class Topology {
public:
  // will create the appropriate topology object
  static Topology* parse(char *);
  virtual void parse(ifstream&) = 0;

  virtual latency_t latency(IPAddress, IPAddress) = 0;

protected:
  Topology();
  virtual ~Topology();
};

#endif //  __TOPOLOGY_H
