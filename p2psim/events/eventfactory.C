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
 */

#include "eventfactory.h"
#include "p2pevent.h"
#include "simevent.h"
#include "oldobserveevent.h"
#include <iostream>
using namespace std;

EventFactory *EventFactory::_instance = 0;

EventFactory*
EventFactory::Instance()
{
  if(!_instance)
    _instance = New EventFactory();
  return _instance;
}

EventFactory::EventFactory()
{
}

EventFactory::~EventFactory()
{
}


Event *
EventFactory::create(string type, vector<string> *v, string proto)
{
  Event *e = 0;
  if (type == "node") {
    assert(proto != "");
    e = New P2PEvent(proto, v);
  }
  
  else if(type == "simulator") {
    e = New SimEvent(v);
  }

  else if(type == "observe") {
    assert(proto != "");
    e = New OldobserveEvent(proto, v);
  }
  
  else
    cerr << "unknown event type" << endl;

  return e;
}
