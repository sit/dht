#ifndef __EUCLIDEANGRAPH_H
#define __EUCLIDEANGRAPH_H

// This Topology puts each node at a point on a Euclidean
// plane, connects the nodes in a random graph, and finds shortest
// paths through the graph. The link latencies are equal to
// the Euclidean distance between the nodes. The intent is to
// mimic the Internet more closely than either Euclidean or RandomGraph.

#include <map>
#include <vector>
#include <string>
using namespace std;

#include "topology.h"
#include "node.h"
#include "dvgraph.h"

class EuclideanGraph : public DVGraph {
public:
  EuclideanGraph(vector<string>*);
  ~EuclideanGraph();
  
  virtual void parse(ifstream&);

private:
  static const int degree = 5;       // links per node
  static const int maxlatency = 200; // link latency 0..maxlatency

  struct Coord {
    double _x;
    double _y;
  };

  vector<Coord> _coords;
};

#endif //  __EUCLIDEANGRAPH_H
