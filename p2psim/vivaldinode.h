#ifndef __VIVALDINODE_H
#define __VIVALDINODE_H

#include "node.h"

// just like a node, but can does guesses as to its own location
class VivaldiNode : public Node {
public:
  VivaldiNode(IPAddress);
  ~VivaldiNode();
};

#endif // __VIVALDINODE_H
