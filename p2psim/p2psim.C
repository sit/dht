#include "eventqueue.h"
#include "network.h"
#include "protocolfactory.h"
#include "eventfactory.h"
#include "threadmanager.h"

#include "p2psim.h"
using namespace std;

unsigned verbose = 0;

Time
now() {
  return EventQueue::Instance()->time();
}

void
graceful_exit(void*)
{
  extern int anyready();

  // send an exit packet to the Network
  unsigned e = 1;
  send(EventQueue::Instance()->exitchan(), &e); // doesn't delete!

  delete EventFactory::Instance();
  delete ProtocolFactory::Instance();
  delete ThreadManager::Instance();
  send(Network::Instance()->exitchan(), 0);
  while(anyready())
    yield();
  __tmg_dmalloc_stats();

  // XXX: can't come before, but I don't understand why not
  delete EventQueue::Instance();
}

