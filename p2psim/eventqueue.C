#include "eventqueue.h"
#include <fstream>
#include "parse.h"
#include "eventfactory.h"
#include "protocolfactory.h"
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
  _eventchan = chancreate(sizeof(Event*), 0);
  assert(_eventchan);
  _gochan = chancreate(sizeof(Event*), 0);
  assert(_gochan);
  thread();
}


EventQueue::~EventQueue()
{
  // delete the entire queue and say bye bye
  eq_entry *next = 0;
  for(eq_entry *cur = _queue.first(); cur; cur = next) {
    next = _queue.next(cur);
    for(vector<Event*>::const_iterator i = cur->events.begin(); i != cur->events.end(); ++i)
      delete (*i);
    delete cur;
  }
  chanfree(_eventchan);
  chanfree(_gochan);
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
  Event *e = 0;
  extern int anyready();

  // Wait for threadmain() to call go().
  recvp(_gochan);

  while(true) {
    // let others run
    while(anyready())
      yield();

    // process any waiting events-to-be-scheduled
    if((e = (Event*) nbrecvp(_eventchan)) != 0) {
      assert(e->ts);
      add_event(e);
      continue;
    }
                                                                                  
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
  // don't advance if we should be exiting
  // if(nbrecvp(_exitchan) != 0) {
  unsigned *x;
  if((x = (unsigned *) nbrecvp(_exitchan)) != 0) {
    delete this;
    return false;
  }

  if(!_queue.size())
    return false;


  // XXX: time is not running smoothly. does that matter?
  eq_entry *eqe = _queue.first();
  assert(eqe);
  for(vector<Event*>::const_iterator i = eqe->events.begin(); i != eqe->events.end(); ++i) {
    assert((*i)->ts == eqe->ts &&
           (*i)->ts >= _time &&
           (*i)->ts < _time + 100000000);
    Event::Execute(*i); // new thread, execute(), delete Event
  }
  _time = eqe->ts;

  _queue.remove(eqe->ts);
  delete eqe;

  if(!_queue.size())
    return false;

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
 // assert(ee->ts);
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


void
EventQueue::parse(char *file)
{
  ifstream in(file);

  if(!in) {
    cerr << "no such file " << file << endl;
    threadexitsall(0);
  }

  string line;

  set<string> allprotos = ProtocolFactory::Instance()->getnodeprotocols();
  if (allprotos.size() > 1) {
    cerr << "warning: running two diffferent protocols on the same node" << endl;
  }

  while(getline(in,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // create the appropriate event type (based on the first word of the line)
    // and let that event parse the rest of the line
    string event_type = words[0];
    words.erase(words.begin());
    for (set<string>::const_iterator i = allprotos.begin(); i != allprotos.end(); ++i) {
      Event *e = EventFactory::Instance()->create(event_type, &words, *i);
      assert(e);
      add_event(e);
    }
  }
}
