#include "protocolfactory.h"
#include "chord.h"
#include "kademlia.h"
#include "pastry.h"
#include "koorde.h"
#include "vivalditest.h"
#include <typeinfo>
#include <iostream>
#include "p2psim.h"


ProtocolFactory *ProtocolFactory::_instance = 0;

ProtocolFactory*
ProtocolFactory::Instance()
{
  if(!_instance)
    _instance = new ProtocolFactory();
  return _instance;
}

void
ProtocolFactory::DeleteInstance()
{
  if(!_instance)
    return;
  delete _instance;
}


ProtocolFactory::ProtocolFactory()
{
}

ProtocolFactory::~ProtocolFactory()
{
}


Protocol *
ProtocolFactory::create(string s, Node *n)
{
  Protocol *p = 0;
  if(s == "Chord") {
    p = new Chord(n);
    _protnames[typeid(Chord).name()] = s;
  } else if (s == "Kademlia") {
    p = new Kademlia(n);
    _protnames[typeid(Kademlia).name()] = s;
  } else if (s == "Pastry") {
    p = new Pastry(n);
    _protnames[typeid(Pastry).name()] = s;
  } else if (s == "Koorde") {
    p = new Koorde(n);
    _protnames[typeid(Koorde).name()] = s;
  } else if (s == "VivaldiTest") {
    p = new VivaldiTest(n);
    _protnames[typeid(Koorde).name()] = s;
  } 
  return p;
}

string
ProtocolFactory::name(Protocol *s)
{
  return _protnames[typeid(*s).name()];
}
