#include "p2psim.h"
#include "eventqueue.h"
#include "network.h"

Time
now() {
  return EventQueue::Instance()->time();
}

Node*
ip2node(IPAddress a)
{
  return Network::Instance()->getnode(a);
}
