#include "eventqueue.h"
#include "network.h"
#include "p2pevent.h"
#include <iostream>
#include <fstream>
#include <list>
using namespace std;

EventQueue *EventQueue::_instance = 0;

EventQueue*
EventQueue::Instance()
{
  if(_instance)
    return _instance;
  return (_instance = new EventQueue());
}


EventQueue::EventQueue() : _time(0)
{
  _eventchan = chancreate(sizeof(Event*), 0);
  assert(_eventchan);
  thread();
}


EventQueue::~EventQueue()
{
}

void
EventQueue::run()
{
  Alt a[2];
  Event *e;

  a[0].c = _eventchan;
  a[0].v = &e;
  a[0].op = CHANRCV;

  while(1) {
    // NB: this is pretty essential.
    // block, (i.e., yield) if the queue is empty.
    a[1].op = _queue.empty() ? CHANEND : CHANNOBLK;

    int i;
    if((i = alt(a)) < 0) {
      cerr << "interrupted" << endl;
      continue;
    }

    switch(i) {
      case 0:
        add_event(e);
        break;

      // nothing's ready. push time forward to the next event
      case 1:
        advance();
        break;

      default:
        break;
    }
  }
}


// moves time forward to the next event
void
EventQueue::advance()
{
  // if there's nothing on the queue something is wrong, I think.
  if(_queue.empty()) {
    cerr << "no more events to run" << endl;
    exit(0);
  }

  // get time of next event.
  // XXX: time is not running smoothly. does that matter?
  Event *e = _queue.front();
  _time = e->ts;
  cout << "*** TIME IS NOW " << _time << " *** " << endl;

  // now process all events with this timestamp
  Queue::iterator pos;
  for(pos = _queue.begin(); pos != _queue.end(); ++pos) {
    if((*pos)->ts > _time) {
      break;
    }
    (*pos)->execute();
  }

  // remove processed events from queue
  _queue.erase(_queue.begin(), pos);
}


void
EventQueue::add_event(Event *e)
{
  // time in event is offset always.  set to absolute time.
  e->ts += _time;

  // empty queue
  if(_queue.empty()) {
    _queue.push_back(e);
    return;
  }

  // find the location where to insert
  Queue::iterator pos;
  for(pos = _queue.begin(); pos != _queue.end(); ++pos)
    if((*pos)->ts > e->ts)
      break;

  _queue.insert(pos, e);
}


void
EventQueue::dump()
{
  cout << "List: ";
  Queue::iterator pos;
  for(pos = _queue.begin(); pos != _queue.end(); ++pos)
    cout << (*pos)->id() << ":" << (*pos)->ts << ", ";
  cout << endl;
}


void
EventQueue::parse(char *file)
{
  ifstream in(file);

  // XXX: this is bullshit. make better.
  while(!in.eof()) {
    int ts, id, event;
    string proto;

    in.setf(ios::dec);
    in >> ts >> id >> event;

    // XXX: WHY?
    if(in.eof())
      break;
    in.unsetf(ios::dec);
    in >> proto;

    P2PEvent *e = new P2PEvent();
    e->ts = (Time) ts;
    e->node = Network::Instance()->getnode((IPAddress) id);
    assert(e->node);
    e->protocol = proto;
    e->event = (Protocol::EventID) event;
    e->args = 0;
    add_event(e);
  }
}
