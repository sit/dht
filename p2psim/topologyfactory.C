#include "topologyfactory.h"
#include "euclidean.h"
#include "e2egraph.h"
#include "g2graph.h"
#include "randomgraph.h"
#include "euclideangraph.h"

Topology *
TopologyFactory::create(string s, vector<string>* v)
{
  Topology *t = 0;

  if(s == "Euclidean")
    t = New Euclidean(v);

  if(s == "RandomGraph")
    t = New RandomGraph(v);

  if(s == "EuclideanGraph")
    t = New EuclideanGraph(v);

  if (s == "E2EGraph")
    t = New E2EGraph(v);

  if (s == "G2Graph")
    t = New G2Graph(v);

  return t;
}
