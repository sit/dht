#include "kademlia.h"
#include "pastry.h"
#include "koorde.h"
#include "chordfingerpns.h"
#include "vivalditest.h"
#include "chordtoe.h"
#include "chordonehop.h"

extern uint base;
extern uint resilience;
extern uint successors;
extern uint fingers;

ProtocolFactory *ProtocolFactory::_instance = 0;

ProtocolFactory*
ProtocolFactory::Instance()
{
  if(!_instance)
    _instance = New ProtocolFactory();
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
  Args a;
  if(_protargs.find(s) != _protargs.end())
    a = _protargs[s];

  if(s == "Chord")
    p = New Chord(n, a);
  if (s == "ChordFinger")
    p = New ChordFinger(n, a);
  if (s == "ChordFingerPNS")
    p = New ChordFingerPNS(n, a);
  if (s == "ChordToe")
    p = New ChordToe(n, a);
  if (s == "ChordOneHop") 
    p = New ChordOneHop(n,a);
  if (s == "Kademlia")
    p = New Kademlia(n, a);
  if (s == "Pastry")
    p = New Pastry(n);
  if (s == "Koorde")
    p = New Koorde(n, a);
  if (s == "VivaldiTest")
    p = New VivaldiTest(n);
  
  assert(p);

  return p;
}

void
ProtocolFactory::setprotargs(string p, Args a)
{
  _protargs[p] = a;
  _protocols.insert(p);
}

set<string>
ProtocolFactory::getnodeprotocols()
{
  return _protocols;
}
