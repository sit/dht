#include "topologyfactory.h"
#include "p2psim.h"

Topology *
TopologyFactory::create(string s)
{
  Topology *t = 0;

  if(s == "Euclidean") {
    t = new Euclidean();
  }

  return t;
}
