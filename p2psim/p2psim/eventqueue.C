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

#include "eventqueue.h"
#include <iostream>
using namespace std;

EventQueue *EventQueue::_instance = 0;

EventQueue*
EventQueue::Instance()
{
  if(_instance)
    return _instance;
  return (_instance = New EventQueue());
}


EventQueue::EventQueue() : _time(0)
{
  _gochan = chancreate(sizeof(Event*), 0);
  assert(_gochan);
  thread();
}


EventQueue::~EventQueue()
{
  // delete the entire queue and say bye bye
  unsigned n = 0;
  eq_entry *next = 0;
  for(eq_entry *cur = _queue.first(); cur; cur = next) {
    next = _queue.next(cur);
    for(vector<Event*>::const_iterator i = cur->events.begin(); i != cur->events.end(); ++i) {
      delete (*i);
      n++;
    }
    delete cur;
  }
  chanfree(_gochan);
  DEBUG(1) << "There were " << n << " outstanding events in the EventQueue." << endl;
}


// Signal to start processing events, main calls
// this just once.
void
EventQueue::go()
{
  send(_gochan, 0);
}


void
EventQueue::run()
{
  // Wait for threadmain() to call go().
  recvp(_gochan);

  while(true) {
    // let others run
    while(anyready())
      yield();

    // time is going to move forward.

    // everyone else is quiet.
    // must be time for the next event.
    // run events for next time in the queue
    if(!advance())
      break;
  }
}

// moves time forward to the next event
bool
EventQueue::advance()
{
  if(!_queue.size())
    return false;

  // Remove the events for the current time and advance the
  // time *before* executing the events, so that the events
  // can correctly call now() and add_event().

  eq_entry *eqe = _queue.first();
  assert(eqe);
  _time = eqe->ts;
  _queue.remove(eqe->ts);
  for(vector<Event*>::const_iterator i = eqe->events.begin(); i != eqe->events.end(); ++i) {
    assert((*i)->ts == eqe->ts &&
           (*i)->ts >= _time &&
           (*i)->ts < _time + 100000000);
    // notify observers, who will not add events
    // into the eventqueue using EventQueueObserver::add_event
    notifyObservers((ObserverInfo*) *i);
    Event::Execute(*i); // new thread, execute(), delete Event
  }
  delete eqe;

  if(!_queue.size()) {
    cout << "queue empty" << endl;
    return false;
  }

  return true;
}


void
EventQueue::add_event(Event *e)
{
  assert(e->ts >= _time);

  eq_entry *ee = 0;
  if(!(ee = _queue.search(e->ts))) {
    ee = New eq_entry(e);
    assert(ee);
    bool b = _queue.insert(ee);
    assert(b);
  }

  //assert(ee->ts);
  //assert(e->ts);
  ee->events.push_back(e);

#if 0
  // empty queue
  if(!_size) {
    _queue.insert(ee);
    _size++;
    return;
  }
  // find the location where to insert
  for(eq_entry *cur = _queue.first(); cur; cur = next) {
    if((*pos)->ts > e->ts)
      break;

  _queue.insert(pos, e);
  _size++;
#endif
}


void
EventQueue::dump()
{
  cout << "List: " << endl;
  eq_entry *next = 0;
  for(eq_entry *cur = _queue.first(); cur; cur = next) {
    cout << "*** Vector with ts = " << cur->ts << " ***" << endl;
    for(vector<Event*>::const_iterator i = cur->events.begin(); i != cur->events.end(); ++i)
      cout << (*i)->id() << ":" << (*i)->ts << ", ";
    cout << endl;
    next = _queue.next(cur);
  }
  cout << endl;
}
