/*
 * Copyright (c) 2003 Thomer M. Gil (thomer@csail.mit.edu)
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

#include "churneventgenerator.h"
#include "p2psim/eventqueue.h"
#include "events/p2pevent.h"

ChurnEventGenerator::ChurnEventGenerator(Args *args) : _size(0)
{
  cout << "ChurnEventGenerator, size = " << (*args)["size"] << endl;
  _size = args->nget("size", 512, 10);
  _proto = (*args)["proto"];
  EventQueue::Instance()->registerObserver(this);
}

void
ChurnEventGenerator::run()
{
  EventQueue::Instance()->go();
}


void
ChurnEventGenerator::kick(Observed *o, ObserverInfo *oi)
{
  cout << "ChurnEventGenerator" << endl;
  P2PEvent *e = New P2PEvent(now() + 1, _proto, (IPAddress) _size+1, "join");
  add_event(e);
}
