#include "protocolfactory.h"
#include "chord.h"
#include "kademlia.h"
#include "pastry.h"
#include "koorde.h"
#include "chordfinger.h"
#include "vivalditest.h"
#include <typeinfo>
#include <iostream>
#include "p2psim.h"
using namespace std;


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

  if(s == "Chord")
    p = new Chord(n);
  if (s == "Kademlia")
    p = new Kademlia(n);
  if (s == "Pastry")
    p = new Pastry(n);
  if (s == "Koorde")
    p = new Koorde(n);
  if (s == "VivaldiTest")
    p = new VivaldiTest(n);
  if (s == "ChordFinger")
    p = new ChordFinger(n);

  assert(p);
  _protnames[typeid(*p).name()] = s;
  return p;
}

string
ProtocolFactory::name(Protocol *s)
{
  return _protnames[typeid(*s).name()];
}
