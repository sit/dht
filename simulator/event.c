/*
 *
 * Copyright (C) 2001 Ion Stoica (istoica@cs.berkeley.edu)
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include <stdio.h>
#include <stdlib.h>

#include "incl.h"

// use a calendar queue to maintain events

Event **initEventQueue()
{
  Event **q;

  if (!(q = (Event **)calloc(1, MAX_NUM_ENTRIES*sizeof(Event **))))
    panic("initEventQueue: memory alloc. error.\n");

  return q;
}


Event *newEvent(int nodeId, void (*fun)(), void *params, double time)
{
  Event *ev;

  if (!(ev = (Event *)calloc(1, sizeof(Event))))
    panic("newEvent: memory alloc. error.\n");

  ev->nodeId = nodeId;
  ev->fun = fun;
  ev->params = params;
  ev->time = time;

  return ev;
}


// delete event ev from the calendar queue
void removeEvent(CalQueue *evCal, Event *ev)
{
  int idx;
  Event *p;

  // get entry to insert the new event 
  idx = ((int)ev->time/ENTRY_TUNIT) % evCal->size;

  p = evCal->q[idx];

  if (p == ev) {
    evCal->q[idx] = ev->next;
    free(ev);
  }

  for (; p->next; p = p->next) {
    if (p->next == ev) {
      p->next = p->next->next;
      free(ev);
    }
  }
}

    
// insert new event in the calendar queue 
void insertEvent(CalQueue *evCal, Event *ev, double time) 
{
  int idx;
  Event *p;

  if (time > MAX_TIME)
    return;

  if (evCal->time > time)
    panic("Insert Event: event inserted in the past\n");

  // get calendar queue entry to insert the new event 
  idx = ((int)time/ENTRY_TUNIT) % evCal->size;

  if (idx >= evCal->size)
    panic("insertEvent: exceed calendar queue size\n");

  ev->time = time;
  ev->next = NULL;

  // insert event at the tail of the queue in the corresponding entry 
  p = evCal->q[idx];
  if (!p) 
    evCal->q[idx] = ev;
  else {
    for (p; p->next; p = p->next);
    p->next = ev;
  }
}


// return next event from the calendar queue
Event *getEvent(CalQueue *evCal, double time)
{
  Event *p, *q;
  int idx = ((int)time/ENTRY_TUNIT) % evCal->size;
  double minTime;
  static int oldIdx = 0;

  if (idx - oldIdx > 1) 
    panic("getEvent: step over an entry!\n");
  else
    oldIdx = idx;

  evCal->time = time;

  if (!evCal->q[idx])
    return NULL;

  p = evCal->q[idx];

  minTime = p->time;

  for (q = p->next; q; q = q->next) {
    if (q->time < minTime)
      minTime = q->time;
    
  }

  if (minTime > time + ENTRY_TUNIT)
    return NULL;

  if (p->time <= minTime) {
    evCal->q[idx] = p->next;
    return p;
  }
  for (q = p; q->next; q = q->next) {

    if (q->next->time <= minTime) {
      p = q->next;
      q->next = q->next->next;
      return p;
    }
  }


  return NULL;
}     


// create a new event and insert in in teh calendar queue
void genEvent(int nodeId, void (*fun)(), void *params, double time)
{
  Event *ev;

  ev = newEvent(nodeId, fun, params, time);

  insertEvent(&EventQueue, ev, time);
}






