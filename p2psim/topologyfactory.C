#include "topologyfactory.h"
#include "euclidean.h"
#include "e2egraph.h"
#include "randomgraph.h"
#include "euclideangraph.h"
#include "p2psim.h"

Topology *
TopologyFactory::create(string s, vector<string>* v)
{
  Topology *t = 0;

  if(s == "Euclidean") {
    t = new Euclidean(v);
  }
  if(s == "RandomGraph") {
    t = new RandomGraph(v);
  }
  if(s == "EuclideanGraph") {
    t = new EuclideanGraph(v);
  }
  if (s == "E2EGraph") {
    t = new E2EGraph(v);
  }
  return t;
}
