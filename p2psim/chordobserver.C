#include "chordobserver.h"
#include "protocol.h"
#include "args.h"
#include "network.h"
#include <iostream>
#include <list>

#include "chord.h"

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
  cout << "Instantiated with jinyang = " << (*a)["jinyang"] << endl;
}

ChordObserver::~ChordObserver()
{
}

void
ChordObserver::execute()
{
  list<Protocol*> l = Network::Instance()->getallprotocols("Chord");
  list<Protocol*>::iterator pos;
  Chord* c;
  int n = 0;
  for (pos = l.begin(); pos != l.end(); ++pos) {
    n++;
    c = (Chord *)(*pos);
    assert(c);
    if (!c->stabilized()) {
      cout << now() << " NOT STABILIZED" << endl;
      return;
    }
  }

  cout << now() << " STABILIZED" << endl;
  for (pos = l.begin(); pos != l.end(); ++pos) {
    assert(c);
    c = (Chord *)(*pos);
    c->dump();
  }
}
