#ifndef __TOP_FACTORY_H
#define __TOP_FACTORY_H

#include "topology.h"
#include <string>
using namespace std;

class TopologyFactory {
public:
  static Topology *create(string s, vector<string>*);
};

#endif // __TOP_FACTORY_H
