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

  virtual Time latency(IPAddress, IPAddress) = 0;
  virtual ~Topology();

protected:
  Topology();
};

#endif //  __TOPOLOGY_H
