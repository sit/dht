#ifndef __EVENTQUEUE_H
#define __EVENTQUEUE_H

#include "event.h"
#include "thread.h"
#include <list>
using namespace std;

class EventQueue : public Threaded {
public:
  static EventQueue* Instance();

  EventQueue();
  ~EventQueue();
  void parse(char*);
  Time time() { return _time; }
  Channel* eventchan() { return _eventchan; }

private:
  typedef list<Event*> Queue;

  static EventQueue *_instance;
  Time _time;
  Queue _queue;
  Channel *_eventchan;

  virtual void run();
  void add_event(Event*);
  void advance();

  void graceful_exit();

  // for debuging
  void dump();
};
 
#endif // __EVENTQUEUE_H
