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

#ifndef __EVENT_H
#define __EVENT_H

#include "node.h"
using namespace std;

class Event {
public:
  friend class EventQueue;

  Event();
  Event(vector<string>*);
  Event(Time, bool fork);

  Time ts;  // absolute time
  unsigned id() { return _id; }
  bool forkp() { return _fork; }
  static void Execute(Event *e);

 protected:
  virtual ~Event();

 private:
  unsigned _id;
  static unsigned _uniqueid;
  bool _fork; // does subclass always want a new thread?

  // Rule: when execute() finishes, the Event must be
  // finished. This is to simplify the decision of when to free.
  // Also you're guaranteed that execute() runs in its own thread.
  virtual void execute() = 0;
  static void Execute1(void *e);
};

#endif // __EVENT_H
