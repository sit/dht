#include "network.h"
#include "oldobserverfactory.h"
#include "eventfactory.h"
#include "threadmanager.h"

unsigned verbose = 0;

Time
now() {
  return EventQueue::Instance()->time();
}

// New plan for crash-free exit:
// Goal:
//   Call destructors before exit() so that they get
//   a chance to print out statistics.
// Rules:
//   After graceful_exit() starts, no other threads will run.
//   Nobody is allowed to call send() or wait or context
//   switch within a destructor.
void
graceful_exit(void*)
{
  delete OldobserverFactory::Instance();
  delete Network::Instance(); // deletes nodes, protocols
  __tmg_dmalloc_stats();

  threadexitsall(0);
}

