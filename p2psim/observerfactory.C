#include "observerfactory.h"
#include "chordobserver.h"
#include "kademliaobserver.h"
#include "tapestryobserver.h"
#include <iostream>
using namespace std;

ObserverFactory* ObserverFactory::_instance = 0;

ObserverFactory *
ObserverFactory::Instance()
{
  if(!_instance)
    _instance = New ObserverFactory();
  return _instance;
}

ObserverFactory::ObserverFactory()
{
}

ObserverFactory::~ObserverFactory()
{
  for(set<Observer*>::const_iterator i = _observers.begin(); i != _observers.end(); ++i)
    delete *i;
}


Observer *
ObserverFactory::create(string s, Args *a)
{
  Observer *t = 0;

  // add a "type" parameter to the argument so that the observer knows that
  // protocol to observe
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
    cerr << "No such observer " << s << endl;
    assert(false);
  }

  _observers.insert(t);
  return t;
}
