#include "protocolfactory.h"
#include "chord.h"
#include "kademlia.h"
#include "pastry.h"
#include "koorde.h"
#include "chordfinger.h"
#include "chordfingerpns.h"
#include "vivalditest.h"
#include <typeinfo>
#include <iostream>
#include "p2psim.h"
#include "chordtoe.h"

using namespace std;

extern uint base;
extern uint resilience;
extern uint successors;
extern uint fingers;

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
  Args a;
  if(_protargs.find(s) != _protargs.end())
    a = _protargs[s];

  if(s == "Chord")
    p = new Chord(n, successors);
  if (s == "ChordFinger")
    p = new ChordFinger(n, base, (successors>resilience? successors:resilience), fingers);
  if (s == "ChordFingerPNS")
    p = new ChordFingerPNS(n,base,successors);
  if (s == "ChordToe")
    p = new ChordToe(n, base, successors, fingers);
  if (s == "Kademlia")
    p = new Kademlia(n, a);
  if (s == "Pastry")
    p = new Pastry(n);
  if (s == "Koorde")
    p = new Koorde(n, base, successors, resilience, fingers);
  if (s == "VivaldiTest")
    p = new VivaldiTest(n);
  
  assert(p);

  return p;
}

void
ProtocolFactory::setprotargs(string p, Args a)
{
  _protargs[p] = a;
}
