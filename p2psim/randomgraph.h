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

class RandomGraph : public Topology {
public:
  RandomGraph();
  ~RandomGraph();
  
  virtual void parse(ifstream&);
  virtual latency_t latency(Node*, Node*);

private:
  int _initialized;
  int _n; // number of nodes
  
  // we index nodes starting at zero.
  map<IPAddress, int> _ip2i;
  vector<Node*> _i2node;

  // 2-d matrix of link delays, -1 for none
  short *_links;
  short& links(int i, int j) { return _links[i*_n + j]; }
  
  // per-node forwarding tables
  struct Entry {
    int _next;
    int _metric;
  };
  Entry *_routes;
  Entry& routes(int i, int j) { return _routes[i*_n + j]; }

  void initialize();
};

#endif //  __RANDOMGRAPH_H
