#include "oldobserveevent.h"
#include "oldobserverfactory.h"
#include "parse.h"

OldobserveEvent::OldobserveEvent()
{
}


OldobserveEvent::OldobserveEvent(Oldobserver *o)
{
  this->_oldobserver = o;
}

OldobserveEvent::OldobserveEvent(string proto, vector<string> *v) : Event(v)
{
  // create a map for the arguments
  Args *a = New Args;
  for(unsigned int i = 0; i < v->size(); i++) {
    vector<string> arg = split((*v)[i], "=");
    a->insert(make_pair(arg[0], arg[1]));
  }
  this->_oldobserver = OldobserverFactory::Instance()->create(proto, a);
  delete a;
}


OldobserveEvent::~OldobserveEvent()
{
}

void
OldobserveEvent::execute()
{
  assert (_oldobserver);
  _oldobserver->execute();
}
