#ifndef __EUCLIDIAN_H
#define __EUCLIDIAN_H

#include <map>
#include <fstream>
using namespace std;

#include "topology.h"
#include "node.h"

class Euclidian : public Topology {
public:
  Euclidian();
  ~Euclidian();
  
  typedef pair<unsigned, unsigned> Coord;
  virtual void parse(ifstream&);
  virtual latency_t latency(Node*, Node*);
  Coord getcoords(NodeID n) { return _nodes[n]; }

private:
  map<NodeID, Coord> _nodes;
  Channel *_distchan;   // to request distances
};

#endif //  __EUCLIDIAN_H
