#include "eventfactory.h"
#include "p2pevent.h"
#include "simevent.h"
#include <iostream>
using namespace std;

EventFactory *EventFactory::_instance = 0;

EventFactory*
EventFactory::Instance()
{
  if(!_instance)
    _instance = new EventFactory();
  return _instance;
}

void
EventFactory::DeleteInstance()
{
  if(!_instance)
    return;
  delete _instance;
}


EventFactory::EventFactory()
{
}

EventFactory::~EventFactory()
{
}


Event *
EventFactory::create(string type, vector<string> *v)
{
  Event *e = 0;
  if(type == "node")
    e = new P2PEvent(v);
  else if(type == "simulator")
    e = new SimEvent(v);
  return e;
}
