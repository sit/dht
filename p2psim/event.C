#include "event.h"
#include <vector>
#include <string>

using namespace std;

unsigned Event::_uniqueid = 0;

Event::Event()
{
  _id = _uniqueid++;
}


Event::~Event()
{
}


Event::Event(vector<string> *v)
{
  _id = _uniqueid++;
  this->ts = (Time) atoi((*v)[0].c_str());
  v->erase(v->begin());
}

// Call Execute(), not execute(), to enforce the free-ing rule,
// and to create the event's thread.
void
Event::Execute(Event *e)
{
  threadcreate(Event::Execute1, e, mainstacksize);
}

void
Event::Execute1(Event *e)
{
  e->execute();
  delete e;
  threadexits(0);
}
