#include "topologyfactory.h"
#include "euclidean.h"
#include "e2egraph.h"
#include "randomgraph.h"
#include "euclideangraph.h"
#include "p2psim.h"

Topology *
TopologyFactory::create(string s, unsigned int num)
{
  Topology *t = 0;

  if(s == "Euclidean") {
    t = new Euclidean();
  }
  if(s == "RandomGraph") {
    t = new RandomGraph();
  }
  if(s == "EuclideanGraph") {
    t = new EuclideanGraph();
  }
  if (s == "E2EGraph") {
    t = new E2EGraph(num);
  }
  return t;
}
