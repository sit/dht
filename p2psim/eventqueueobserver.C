#include "eventqueueobserver.h"
#include "eventqueue.h"

void
EventQueueObserver::add_event(Event *e)
{
  // private access through friendship
  EventQueue::Instance()->add_event(e);
}
