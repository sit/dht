#include "topologyfactory.h"

Topology *
TopologyFactory::create(string s)
{
  Topology *t = 0;

  if(s == "Euclidian") {
    t = new Euclidian();
  }

  return t;
}
