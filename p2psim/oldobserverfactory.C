#include "oldobserverfactory.h"
#include "chordobserver.h"
#include "kademliaobserver.h"
#include "tapestryobserver.h"
#include <iostream>
using namespace std;

OldobserverFactory* OldobserverFactory::_instance = 0;

OldobserverFactory *
OldobserverFactory::Instance()
{
  if(!_instance)
    _instance = New OldobserverFactory();
  return _instance;
}

OldobserverFactory::OldobserverFactory()
{
}

OldobserverFactory::~OldobserverFactory()
{
  for(set<Oldobserver*>::const_iterator i = _oldobservers.begin(); i != _oldobservers.end(); ++i)
    delete *i;
}


Oldobserver *
OldobserverFactory::create(string s, Args *a)
{
  Oldobserver *t = 0;

  // add a "type" parameter to the argument so that the oldobserver knows that
  // protocol to oldobserve
  string type = OBSERVER_TYPE;
  a->insert(make_pair(type, s));

  if((s == "Chord") ||
     (s == "ChordFinger") ||
     (s == "ChordFingerPNS") ||
     (s == "ChordToe") ||
     (s == "ChordOneHop") ||
     (s == "Koorde"))
  {
    t = ChordObserver::Instance(a);
  }

  else if(s == "Kademlia") {
    t = KademliaObserver::Instance(a);
  }

  else if(s == "Tapestry") {
    t = TapestryObserver::Instance(a);
  }
  
  else {
    cerr << "No such oldobserver " << s << endl;
    assert(false);
  }

  _oldobservers.insert(t);
  return t;
}
