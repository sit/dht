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


EventQueue::EventQueue() : _time(0), _size(0)
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
  for(Queue::iterator pos = _queue.begin(); pos != _queue.end(); ++pos)
    delete *pos;
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
    if(!_size)
      ::graceful_exit();
                                                                                  
    // run events for next time in the queue
    advance();
  }
}


bool
EventQueue::should_exit()
{
  Alt a[2];
  unsigned exit;
  
  a[0].c = _exitchan;
  a[0].v = &exit;
  a[0].op = CHANRCV;
  a[1].op = CHANNOBLK;

  unsigned i = 0;
  if((i = alt(a)) < 0) {
    cerr << "interrupted" << endl;
    assert(false);
  }
  return i == 0;
}

// moves time forward to the next event
void
EventQueue::advance()
{
  if(!_size) {
    cerr << "queue is empty" << endl;
    exit(-1);
  }

  // XXX: time is not running smoothly. does that matter?
  Event *e = _queue.front();
  assert(e->ts >= _time && e->ts < _time + 100000000);
  _time = e->ts;

  while(_size > 0) {
    // is there an exit event?
    if(should_exit())
      delete this;

    Event *first = _queue.front();
    if(first->ts > _time)
      break;
    _queue.pop_front();
    _size--;
    Event::Execute(first); // new thread, execute(), delete Event
  }
}


void
EventQueue::add_event(Event *e)
{
  assert(e->ts >= _time);

  // empty queue
  if(!_size) {
    _queue.push_back(e);
    _size++;
    return;
  }

  // find the location where to insert
  Queue::iterator pos;
  for(pos = _queue.begin(); pos != _queue.end(); ++pos)
    if((*pos)->ts > e->ts)
      break;

  _queue.insert(pos, e);
  _size++;
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
    if ((event_type == "node") || (event_type == "observe")) {
      for (set<string>::const_iterator i = allprotos.begin(); i != allprotos.end(); ++i) {
	Event *e = EventFactory::Instance()->create(event_type, &words, *i);
	assert(e);
	add_event(e);
      }
    } else {
      Event *e = EventFactory::Instance()->create(event_type, &words);
      assert(e);
      add_event(e);
    }
  }
}
