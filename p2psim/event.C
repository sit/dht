#include "event.h"
#include "threadmanager.h"

unsigned Event::_uniqueid = 0;

Event::Event()
{
  _id = _uniqueid++;
}

Event::Event(Time ts)
{
  _id = _uniqueid++;
  this->ts = ts;
}


Event::Event(vector<string> *v)
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
  ThreadManager::Instance()->create(Event::Execute1, e);
}

void
Event::Execute1(void *ex)
{
  Event *e = (Event*) ex;
  e->execute();
  delete e;
  threadexits(0);
}
