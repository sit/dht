#include "vivaldinode.h"
#include "p2psim.h"
#include <stdlib.h>
#include <math.h>

VivaldiNode::VN VivaldiNode::_nodes;

VivaldiNode::VivaldiNode(IPAddress ip) : Node(ip)
{
  _c.first = 10000 + (random() % 200);
  _c.second = 10000 + (random() % 200);
  _nodes[ip] = this;
}

VivaldiNode::~VivaldiNode()
{
}

// measure how long it takes to send the packet
bool
VivaldiNode::sendPacket(IPAddress dst, Packet *p)
{
  Time before = now();

  if(!Node::sendPacket(dst, p))
    return false;

  // get coordinates of destination
  Coord c = _nodes[dst]->getCoords();

  double dx = c.first - _c.first;
  double dy = c.first - _c.second;
  double d = sqrt(dx*dx + dy*dy);

  if(d > 0){
    // move 1/100th of the way towards the position
    // implied by the latency.
    Time latency = now() - before;
    dx = (dx / d) * (d - latency) / 100.0;
    dy = (dy / d) * (d - latency) / 100.0;

    _c.first += dx;
    _c.second += dy;
  }
  
  return true;
}
