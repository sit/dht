#include "tapestryobserver.h"
#include "protocol.h"
#include "args.h"
#include "network.h"
#include <iostream>
#include <list>
#include <algorithm>
#include <stdio.h>

#include "tapestry.h"

using namespace std;

TapestryObserver* TapestryObserver::_instance = 0;

TapestryObserver*
TapestryObserver::Instance(Args *a)
{
  if(!_instance)
    _instance = New TapestryObserver(a);
  return _instance;
}


TapestryObserver::TapestryObserver(Args *a) : Oldobserver(a)
{
  _reschedule = 0;
  _reschedule = atoi((*a)["reschedule"].c_str());
  _num_nodes = atoi((*a)["numnodes"].c_str());
  assert(_num_nodes > 0);

  _init_num = atoi((*a)["initnodes"].c_str());
  lid.clear();
}

TapestryObserver::~TapestryObserver()
{
}

void
TapestryObserver::init_state()
{
  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  DEBUG(1) << "TapestryObserver::init_state " << now() << endl;
  for(list<Protocol*>::iterator pos = l.begin(); pos != l.end(); ++pos) {
    Tapestry *t = (Tapestry*) *pos;
    t->init_state(l);
  }
}

void
TapestryObserver::execute()
{

  if(_init_num) {
    init_state();
    _init_num = 0;
  }

  DEBUG(1) << "TapestryObserver executing" << endl;
  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  list<Protocol*>::iterator pos;

  //i only want to sort it once after all nodes have joined! 
  Tapestry *c = 0;
  if (lid.size() != _num_nodes) {
    lid.clear();
    for (pos = l.begin(); pos != l.end(); ++pos) {
      c = (Tapestry *)(*pos);
      assert(c);
      lid.push_back(c->id ());
    }

    sort(lid.begin(), lid.end());
  }

  for (pos = l.begin(); pos != l.end(); ++pos) {
    c = (Tapestry *)(*pos);
    assert(c);
    if (!c->stabilized(lid)) {
      DEBUG(1) << now() << " NOT STABILIZED" << endl;
      if (_reschedule > 0) reschedule(_reschedule);
      return;
    }

  }

  DEBUG(0) << now() << " STABILIZED" << endl;
}
