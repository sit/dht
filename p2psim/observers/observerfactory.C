/*
 * Copyright (c) 2003-2005 Thomer M. Gil
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

#include "observerfactory.h"
#include "kademliaobserver.h"
#include "tapestryobserver.h"
#include "kelipsobserver.h"
#include "chordobserver.h"
#include "datastoreobserver.h"

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

  assert(a);
  if(s == "KademliaObserver") {
    t = New KademliaObserver(a);
  } else if(s == "TapestryObserver") {
    t = New TapestryObserver(a);
  } else if(s == "KelipsObserver") {
    t = New KelipsObserver(a);
  } else if (s == "ChordObserver") {
    t = New ChordObserver(a);
  } else if (s == "DataStoreObserver") {
    t = New DataStoreObserver (a);
  }
  _observers.insert(t);
  return t;
}
