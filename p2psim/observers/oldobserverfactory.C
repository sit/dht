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

  /*
  else if(s == "Kademlia") {
    t = New KademliaObserver();
  }
  */

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
