#include "chordobserver.h"
#include "protocol.h"
#include "args.h"
#include "network.h"
#include <iostream>
#include <list>

#include "chord.h"
#include "koorde.h"

using namespace std;

ChordObserver* ChordObserver::_instance = 0;

ChordObserver*
ChordObserver::Instance(Args *a)
{
  if(!_instance)
    _instance = new ChordObserver(a);
  return _instance;
}


ChordObserver::ChordObserver(Args *a)
{
  _reschedule = 0;
  _reschedule = atoi((*a)["reschedule"].c_str());
  _type = (*a)["type"];
}

ChordObserver::~ChordObserver()
{
}

void
ChordObserver::execute()
{
  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  list<Protocol*>::iterator pos;
  Chord* c;
  int n = 0;
  for (pos = l.begin(); pos != l.end(); ++pos) {
    n++;
    c = (Chord *)(*pos);
    assert(c);
    if (!c->stabilized()) {
      cout << now() << " NOT STABILIZED" << endl;
      if (_reschedule > 0) reschedule(_reschedule);
      return;
    }
  }

  cout << now() << " STABILIZED" << endl;
  cout << now() << " CHORD NODE STATS" << endl;
  for (pos = l.begin(); pos != l.end(); ++pos) {
    assert(c);
    c = (Chord *)(*pos);
    c->dump();
  }
}
