#include "netevent.h"

NetEvent::NetEvent()
  : Event(0, false)
{
}

NetEvent::~NetEvent()
{
}

void
NetEvent::execute()
{
  send(node->pktchan(), &p);
}
