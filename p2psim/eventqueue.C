#include "eventqueue.h"
#include "network.h"
#include "p2pevent.h"
#include "parse.h"
#include "eventfactory.h"
#include <iostream>
#include <fstream>
#include <list>
#include <stdio.h>
#include "p2psim.h"
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
  _gochan = chancreate(sizeof(Event*), 0);
  assert(_gochan);
  thread();
}


EventQueue::~EventQueue()
{
  // delete me
  chanfree(_eventchan);
  chanfree(_gochan);
  threadexitsall(0);
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
  Alt a[2];

  // Wait for threadmain() to call go().
  a[0].c = _gochan;
  a[0].v = 0;
  a[0].op = CHANRCV;
  a[1].op = CHANEND;
  alt(a);

  while(1){
    // let others run
    while(anyready())
      yield();

    // process any waiting events-to-be-scheduled
    if((e = (Event*)nbrecvp(_eventchan)) != 0){
      add_event(e);
      continue;
    }
                                                                                  
    // everyone else is quiet.
    // must be time for the next event.
                                                                                  
    // no more events.  we're done!
    if(_queue.empty())
      graceful_exit();
                                                                                  
    // run events for next time in the queue
    advance();
  }
}


void
EventQueue::graceful_exit()
{
  extern int anyready();
  cout << "End of simulation, chances are I'm going to segfault now.\n";

  // stop stuff
  send(Network::Instance()->exitchan(), 0);

  // give everyone a chance to clean up
  while(anyready())
    yield();

  ::graceful_exit();

  delete this;
}


// moves time forward to the next event
void
EventQueue::advance()
{
  if(_queue.empty()) {
    cerr << "queue is empty" << endl;
    exit(-1);
  }

  // XXX: time is not running smoothly. does that matter?
  Event *e = _queue.front();
  assert(e->ts >= _time && e->ts < _time + 100000000);
  _time = e->ts;

  while(_queue.size() > 0){
    Event *first = _queue.front();
    if(first->ts > _time)
      break;
    _queue.pop_front();
    Event::Execute(first); // new thread, execute(), delete Event
  }
}


void
EventQueue::add_event(Event *e)
{
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

  if(!in) {
    cerr << "no such file " << file << endl;
    threadexitsall(0);
  }

  string line;
  while(getline(in,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // create the appropriate event type (based on the first word of the line)
    // and let that event parse the rest of the line
    string event_type = words[0];
    words.erase(words.begin());
    Event *e = EventFactory::Instance()->create(event_type, &words);
    assert(e);
    add_event(e);
  }
}
