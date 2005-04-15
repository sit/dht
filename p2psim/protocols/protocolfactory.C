/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu)
 *                    Jinyang Li (jinyang@csail.mit.edu)
 *                    Robert Morris (rtm@csail.mit.edu)
 *                    Frans Kaashoek (kaashoek@csail.mit.edu)
 *                    Jeremy Stribling (strib@csail.mit.edu)
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
#include "koorde.h"
#include "chordfingerpns.h"
#include "misc/vivalditest.h"
#include "misc/datastore.h"
#include "chordtoe.h"
#include "chordonehop.h"
#include "tapestry.h"
#include "kelips.h"
#include "onehop.h"
#include "accordion.h"
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


Node *
ProtocolFactory::create(IPAddress i, const char *name)
{
  Node *p = 0;
  string s = Node::protocol();
  Args a = Node::args();

  if(s == "Chord")
    p = New Chord(i, a);
  if (s == "ChordFinger")
    p = New ChordFinger(i, a);
  if (s == "ChordFingerPNS")
    p = New ChordFingerPNS(i, a, NULL, name); 
  if (s == "ChordToe")
    p = New ChordToe(i, a);
  if (s == "Accordion")
    p = New Accordion(i,a);
  if (s == "OneHop")
    p = New OneHop(i,a);
  if (s == "Kademlia")
    p = New Kademlia(i, a);
  if (s == "Tapestry")
    p = New Tapestry(i, a);
  if (s == "Kelips")
    p = New Kelips(i, a);
  if (s == "Koorde")
    p = New Koorde(i, a);
  if (s == "SillyProtocol")
    p = New SillyProtocol(i, a);
  if (s == "VivaldiTest")
    p = New VivaldiTest(i, a);
  if (s == "DataStore")
    p = New DataStore(i, a);
  
  assert(p);
  return p;
}
