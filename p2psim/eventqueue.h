#ifndef __EVENTQUEUE_H
#define __EVENTQUEUE_H

#include "event.h"
#include "thread.h"
#include <list>
using namespace std;

class EventQueue : public Threaded {
  friend class Observer; // are executed in EventQueue's thread, so allowed.

public:
  static EventQueue* Instance();

  EventQueue();
  ~EventQueue();
  void parse(char*);
  Time time() { return _time; }
  Channel* eventchan() { return _eventchan; }
  void go();

private:
  typedef list<Event*> Queue;

  static EventQueue *_instance;
  Time _time;
  Queue _queue;
  Channel *_eventchan;
  Channel *_gochan;
  unsigned _size; // because list.size() is so slow

  virtual void run();
  void add_event(Event*);
  void advance();
  bool should_exit();

  // for debuging
  void dump();
};
 
#endif // __EVENTQUEUE_H
