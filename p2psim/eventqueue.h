#ifndef __EVENTQUEUE_H
#define __EVENTQUEUE_H

#include "event.h"
#include "../utils/skiplist.h"
using namespace std;

class EventQueue : public Threaded, Observed {
  friend class FileEventGenerator;

  // XXX: remove this
  // are executed in EventQueue's thread, so allowed.
  friend class Oldobserver;

public:
  static EventQueue* Instance();
  Time time() { return _time; }
  Channel* eventchan() { return _eventchan; }
  void go();

private:
  EventQueue();
  ~EventQueue();

  struct eq_entry {
    eq_entry() { ts = 0; events.clear(); }
    eq_entry(Event *e) { ts = e->ts; events.clear(); }
    Time ts;
    vector<Event*> events;
    sklist_entry<eq_entry> _sortlink;
  };
  skiplist<eq_entry, Time, &eq_entry::ts, &eq_entry::_sortlink> _queue;

  static EventQueue *_instance;
  Time _time;
  Channel *_eventchan;
  Channel *_gochan;

  virtual void run();
  void add_event(Event*);
  bool advance();

  // for debuging
  void dump();
};
 
#endif // __EVENTQUEUE_H
