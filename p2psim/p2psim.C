#include "eventqueue.h"
#include "network.h"
#include <iostream>
#include "protocolfactory.h"
#include "eventfactory.h"

#include "p2psim.h"
using namespace std;

unsigned verbose = 0;

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
  extern int anyready();

  // send an exit packet to the Network
  unsigned e = 1;
  send(EventQueue::Instance()->exitchan(), &e);
  send(Network::Instance()->exitchan(), &e);

  while(anyready())
    yield();

  EventFactory::DeleteInstance();
  ProtocolFactory::DeleteInstance();
  threadexitsall(0);
}
