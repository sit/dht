#include "observeevent.h"
#include "eventqueue.h"

Observer::Observer(Args *a)
{
  _type = (*a)[OBSERVER_TYPE];
}

Observer::~Observer()
{
}

void
Observer::reschedule(Time t)
{
  // XXX: this is only safe because Observers are executed in the
  // context of EventQueue's thread.
  ObserveEvent *e = New ObserveEvent(this);
  e->ts = now() + t;
  EventQueue::Instance()->add_event(e);
}
