#include "oldobserveevent.h"
#include "eventqueue.h"

Oldobserver::Oldobserver(Args *a)
{
  _type = (*a)[OBSERVER_TYPE];
}

Oldobserver::~Oldobserver()
{
}

void
Oldobserver::reschedule(Time t)
{
  OldobserveEvent *e = New OldobserveEvent(this);
  e->ts = now() + t;

  // XXX: this is only safe because Oldobservers are executed in the
  // context of EventQueue's thread.  Normally, we would have to use send().
  EventQueue::Instance()->add_event(e);
}
