/*
 * Copyright (c) 2003 [NAMES_GO_HERE]
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#include "protocolfactory.h"
#include "kademlia.h"
#include "pastry.h"
#include "koorde.h"
#include "chordfingerpns.h"
#include "misc/vivalditest.h"
#include "chordtoe.h"
#include "chordonehop.h"
#include "tapestry.h"
#include "kelips.h"
#include "sillyprotocol.h"

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
  if (s == "Tapestry")
    p = New Tapestry(n, a);
  if (s == "Kelips")
    p = New Kelips(n, a);
  if (s == "Koorde")
    p = New Koorde(n, a);
  if (s == "SillyProtocol")
    p = New SillyProtocol(n, a);
  if (s == "VivaldiTest")
    p = New VivaldiTest(n,a);
  
  _allprotocols.insert(p);
  assert(p);

  return p;
}

void
ProtocolFactory::setprotargs(string p, Args a)
{
  _protargs[p] = a;
  _protocols.insert(p);
}

const set<string>
ProtocolFactory::getnodeprotocols()
{
  return _protocols;
}

const set<Protocol*>
ProtocolFactory::getallprotocols()
{
  return _allprotocols;
}
