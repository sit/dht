#include "p2pevent.h"
#include "node.h"
#include "protocol.h"
#include <lib9.h>
#include <thread.h>
#include <iostream>
using namespace std;

P2PEvent::P2PEvent()
{
}

P2PEvent::~P2PEvent()
{
}

void
P2PEvent::execute()
{
  // get node, protocol on that node, application interface for that protocol
  // and invoke the event
  Protocol *proto = node->getproto(protocol);
  assert(proto);
  Channel *c = proto->appchan();
  assert(c);

  // XXX: this seems to be non-blocking.  is that correct?
  P2PEvent *me = this;
  send(c, &me);
}
