#include "topologyfactory.h"

Topology *
TopologyFactory::create(string s)
{
  Topology *t = 0;

  if(s == "Euclidean") {
    t = new Euclidean();
  }

  return t;
}
