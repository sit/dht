#include "eventqueue.h"
#include "network.h"
#include <iostream>
#include "protocolfactory.h"
#include "eventfactory.h"

#include "p2psim.h"
using namespace std;

Time
now() {
  return EventQueue::Instance()->time();
}

Node*
ip2node(IPAddress a)
{
  return Network::Instance()->getnode(a);
}


void
graceful_exit()
{
  EventFactory::DeleteInstance();
  ProtocolFactory::DeleteInstance();
}
