#include "eventfactory.h"
#include "p2pevent.h"
#include "simevent.h"
#include "observeevent.h"
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
EventFactory::create(string type, vector<string> *v, string proto)
{
  Event *e = 0;
  if (type == "node") {
    e = new P2PEvent(proto,v);
  }else if(type == "simulator")
    e = new SimEvent(v);
  else if(type == "observe")
    e = new ObserveEvent(proto,v);
  else
    cerr << "unknown event type" << endl;
  return e;
}
