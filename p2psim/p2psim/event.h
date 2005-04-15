/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu)
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

#ifndef __EVENT_H
#define __EVENT_H

#include "p2psim.h"
#include <string>
#include <vector>
using namespace std;

class Event;

class Event {
public:
  friend class EventQueue;

  Event(string);
  Event(string, vector<string>*);
  Event(string, Time, bool fork);

  Time ts;  // absolute time
  unsigned id() { return _id; }
  string name() { return _name; }
  bool forkp() { return _fork; }
  static void Execute(Event *e);

 protected:
  virtual ~Event();

 private:
  unsigned _id;
  static unsigned _uniqueid;
  bool _fork; // does subclass always want a new thread?
  string _name;

  // Rule: when execute() finishes, the Event must be
  // finished. This is to simplify the decision of when to free.
  // Also you're guaranteed that execute() runs in its own thread.
  virtual void execute() = 0;
  static void Execute1(void *e);
};

#endif // __EVENT_H
