#include "observeevent.h"
#include "observerfactory.h"
#include "parse.h"

ObserveEvent::ObserveEvent()
{
}


ObserveEvent::ObserveEvent(Observer *o)
{
  this->_observer = o;
}

ObserveEvent::ObserveEvent(string proto, vector<string> *v) : Event(v)
{
  // create a map for the arguments
  Args *a = New Args;
  for(unsigned int i = 0; i < v->size(); i++) {
    vector<string> arg = split((*v)[i], "=");
    a->insert(make_pair(arg[0], arg[1]));
  }
  string t = "type";
  a->insert(make_pair(t, proto));
  this->_observer = ObserverFactory::Instance()->create(proto, a);
  delete a;
}


ObserveEvent::~ObserveEvent()
{
}

void
ObserveEvent::execute()
{
  assert (_observer);
  _observer->execute();
}
