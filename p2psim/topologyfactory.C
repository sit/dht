#include "topologyfactory.h"
#include "euclidean.h"
#include "randomgraph.h"
#include "euclideangraph.h"
#include "p2psim.h"

Topology *
TopologyFactory::create(string s)
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

  return t;
}
