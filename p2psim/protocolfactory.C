#include "protocolfactory.h"
#include "chord.h"
#include "kademlia.h"
#include "pastry.h"
#include <typeinfo>
#include <iostream>


ProtocolFactory *ProtocolFactory::_instance = 0;

ProtocolFactory*
ProtocolFactory::Instance()
{
  if(!_instance)
    _instance = new ProtocolFactory();
  return _instance;
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
  } 
  return p;
}

string
ProtocolFactory::name(Protocol *s)
{
  return _protnames[typeid(*s).name()];
}
