/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu),
 *                    Robert Morris (rtm@csail.mit.edu)
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

#include "event.h"
#include "threadmanager.h"

unsigned Event::_uniqueid = 0;

Event::Event( string name )
{
  _id = _uniqueid++;
  this->ts = 0;
  _fork = true;
  _name = name;
}

Event::Event(string name, Time ts, bool fork)
{
  _id = _uniqueid++;
  this->ts = ts;
  _fork = fork;
  _name = name;
}

Event::Event(string name, vector<string> *v)
  : _fork(true)
{
  _id = _uniqueid++;
  this->ts = (Time) strtoull((*v)[0].c_str(), NULL, 10);
  v->erase(v->begin());
  _name = name;
}


Event::~Event()
{
}

// Call Execute(), not execute(), to enforce the free-ing rule,
// and to create the event's thread.
void
Event::Execute(Event *e)
{
  if(e->forkp()){
    ThreadManager::Instance()->create(Event::Execute1, e);
  } else {
    e->execute();
    delete e;
  }
}

void
Event::Execute1(void *ex)
{
  Event *e = (Event*) ex;
  e->execute();
  delete e;
  taskexit(0);
}
