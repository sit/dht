/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu)
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

#include "sillyeventgenerator.h"
#include "events/p2pevent.h"
#include "events/simevent.h"
#include "p2psim/eventqueue.h"
#include "p2psim/network.h"
#include <iostream>
using namespace std;

SillyEventGenerator::SillyEventGenerator(Args *args)
{
  _exittime = (*args)["exittime"];
  assert(_exittime != "");

  _ips = Network::Instance()->getallfirstips();
  EventQueue::Instance()->registerObserver(this);
  _prevtime = 0;
}

SillyEventGenerator::~SillyEventGenerator()
{
  delete _ips;
}

void
SillyEventGenerator::run()
{
  // first register the exit event
  vector<string> simargs;
  simargs.push_back(_exittime);
  simargs.push_back("exit");
  SimEvent *se = New SimEvent(&simargs);
  add_event(se);

  bullshit();
  EventQueue::Instance()->go();
}


void
SillyEventGenerator::kick(Observed *o, ObserverInfo *oi)
{
  if(now() == _prevtime)
    return;

  bullshit();
  _prevtime = now();
}

void
SillyEventGenerator::bullshit()
{
  Args *a = New Args();
  char key[32]; sprintf(key, "%d", (unsigned) (random() % _ips->size())+1);
  (*a)["key"] = string(key);
  P2PEvent *e = New P2PEvent(now()+1, ((random() % _ips->size())+1), "lookup", a);
  add_event(e);
}
