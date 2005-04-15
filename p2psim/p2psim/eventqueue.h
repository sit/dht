/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu)
 *                    Robert Morris (rtm@csail.mit.edu)
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef __EVENTQUEUE_H
#define __EVENTQUEUE_H

#include "threaded.h"
#include "event.h"
#include "observed.h"
#include "skiplist.h"
using namespace std;

class EventQueue : public Threaded, public Observed {
  friend class EventQueueObserver;

public:
  static EventQueue* Instance();
  ~EventQueue();

  void add_event(Event*);
  Time time() { return _time; }
  static Time fasttime() { return _instance?_instance->time():0; }
  void go();

private:
  EventQueue();

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
  Channel *_gochan;

  virtual void run();
  bool advance();

  // for debuging
  void dump();
};
 
#endif // __EVENTQUEUE_H
