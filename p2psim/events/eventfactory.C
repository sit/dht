#include "eventfactory.h"
#include "p2pevent.h"
#include "simevent.h"
#include "oldobserveevent.h"
#include <iostream>
using namespace std;

EventFactory *EventFactory::_instance = 0;

EventFactory*
EventFactory::Instance()
{
  if(!_instance)
    _instance = New EventFactory();
  return _instance;
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
    assert(proto != "");
    e = New P2PEvent(proto, v);
  }
  
  else if(type == "simulator") {
    e = New SimEvent(v);
  }

  else if(type == "observe") {
    assert(proto != "");
    e = New OldobserveEvent(proto, v);
  }
  
  else
    cerr << "unknown event type" << endl;

  return e;
}
