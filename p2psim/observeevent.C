#include "observeevent.h"
#include "observerfactory.h"
#include "parse.h"
#include <iostream>
#include "p2psim.h"
using namespace std;

ObserveEvent::ObserveEvent()
{
}

ObserveEvent::ObserveEvent(vector<string> *v) : Event(v)
{
  // first word is what kind of observer we are
  string type = (*v)[0];

  // create a map for the arguments
  Args *a = new Args;
  for(unsigned int i=1; i<v->size(); i++) {
    vector<string> arg = split((*v)[i], "=");
    a->insert(make_pair(arg[0], arg[1]));
  }
  this->_observer = ObserverFactory::create(type, a);
}


ObserveEvent::~ObserveEvent()
{
}

void
ObserveEvent::execute()
{
  _observer->execute();
}
