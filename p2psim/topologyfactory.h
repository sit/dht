#ifndef __TOP_FACTORY_H
#define __TOP_FACTORY_H

#include "topology.h"
#include <string>

class TopologyFactory {
public:
  static Topology *create(string s, unsigned int num = 0);
};

#endif // __TOP_FACTORY_H
