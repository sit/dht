#include "netevent.h"

NetEvent::NetEvent()
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
