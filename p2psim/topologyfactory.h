#ifndef __TOP_FACTORY_H
#define __TOP_FACTORY_H

#include "topology.h"
#include "euclidean.h"
#include <string>

class TopologyFactory {
public:
  static Topology *create(string);
};

#endif // __TOP_FACTORY_H
