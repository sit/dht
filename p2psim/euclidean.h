#ifndef __EUCLIDEAN_H
#define __EUCLIDEAN_H

#include <map>
#include <fstream>
using namespace std;

#include "topology.h"
#include "node.h"

class Euclidean : public Topology {
public:
  Euclidean();
  ~Euclidean();
  
  typedef pair<unsigned, unsigned> Coord;
  virtual void parse(ifstream&);
  virtual latency_t latency(Node*, Node*);
  Coord getcoords(NodeID n) { return _nodes[n]; }

private:
  map<NodeID, Coord> _nodes;
  Channel *_distchan;   // to request distances
};

#endif //  __EUCLIDEAN_H
