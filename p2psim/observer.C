#include <lib9.h>
#include <thread.h>
#include "observeevent.h"
#include "observer.h"
#include "eventqueue.h"
#include "p2psim.h"

Observer::Observer()
{
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
