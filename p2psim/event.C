#include "event.h"
#include "threadmanager.h"

unsigned Event::_uniqueid = 0;

Event::Event()
{
  _id = _uniqueid++;
  this->ts = 0;
  _fork = true;
}

Event::Event(Time ts, bool fork)
{
  _id = _uniqueid++;
  this->ts = ts;
  _fork = fork;
}

Event::Event(vector<string> *v)
  : _fork(true)
{
  _id = _uniqueid++;
  this->ts = (Time) strtoull((*v)[0].c_str(), NULL, 10);
  v->erase(v->begin());
}


Event::~Event()
{
}

// Call Execute(), not execute(), to enforce the free-ing rule,
// and to create the event's thread.
void
Event::Execute(Event *e)
{
  if(e->forkp()){
    ThreadManager::Instance()->create(Event::Execute1, e);
  } else {
    e->execute();
    delete e;
  }
}

void
Event::Execute1(void *ex)
{
  Event *e = (Event*) ex;
  e->execute();
  delete e;
  threadexits(0);
}
