#ifndef __RANDOMGRAPH_H
#define __RANDOMGRAPH_H

// This Topology simulates a random graph topology among
// the Nodes. That is, every Node has a few links to other
// Nodes, with randomly chosen link latencies. RandomGraph
// computes shortest paths through the graph in order
// to find latencies between Nodes that aren't directly
// connected. RandomGraph makes no attempt to embed the
// Nodes in a plane or any other space. The point is to
// test out Vivaldi &c in a situation far removed from
// Euclidean.

#include <map>
#include <vector>
using namespace std;

#include "topology.h"
#include "node.h"
#include "dvgraph.h"

class RandomGraph : public DVGraph {
public:
  RandomGraph();
  ~RandomGraph();
  
  virtual void parse(ifstream&);

private:
  static const int degree = 5;       // links per node
  static const int maxlatency = 200; // link latency 0..maxlatency
};

#endif //  __RANDOMGRAPH_H
