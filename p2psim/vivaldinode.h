#ifndef __VIVALDINODE_H
#define __VIVALDINODE_H

#include "node.h"
#include "packet.h"
#include "p2psim.h"
#include <map>
using namespace std;

// just like a node, but can does guesses as to its own location
class VivaldiNode : public Node {
public:
  typedef pair<double, double> Coord;

  VivaldiNode(IPAddress);
  ~VivaldiNode();

  bool sendPacket(IPAddress, Packet*);
  Coord getCoords() { return _c; }

private:
  Coord _c;
  typedef map<IPAddress, VivaldiNode*> VN;
  static VN _nodes;
};

#endif // __VIVALDINODE_H
