#ifndef __EVENT_QUEUE_OBSERVER_H
#define __EVENT_QUEUE_OBSERVER_H

#include "observer.h"
class Event;

// is a friend of EventQueue
class EventQueueObserver : public Observer {
protected:
  void add_event(Event*);
};

#endif // __EVENT_QUEUE_OBSERVER_H
