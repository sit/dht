#include <stdio.h>
#include <malloc.h>

#include "incl.h"

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


void removeEvent(CalQueue *evCal, Event *ev)
{
  int idx;
  Event *p;

  /* compute entry where to insert the new event */
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

    
/* insert new event in the queue */
void insertEvent(CalQueue *evCal, Event *ev, double time) 
{
  int idx;
  Event *p;

  if (time > MAX_TIME)
    return;

  if (evCal->time > time)
    panic("Insert Event: event inserted in the past\n");

  /* compute entry where to insert the new event */
  idx = ((int)time/ENTRY_TUNIT) % evCal->size;

  if (idx >= evCal->size)
    panic("insertEvent: exceed calendar queue size\n");

  ev->time = time;
  ev->next = NULL;

  /* insert event at the tail of the queue in the corresponding entry */
  p = evCal->q[idx];
  if (!p) 
    evCal->q[idx] = ev;
  else {
    for (p; p->next; p = p->next);
    p->next = ev;
  }
  /*
  ev->next = p;
  ev->time = time;
  evCal->q[idx] = ev;
  */
}


/* delete event from queueu */
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


void genEvent(int nodeId, void (*fun)(), void *params, double time)
{
  Event *ev;

  ev = newEvent(nodeId, fun, params, time);

  insertEvent(&EventQueue, ev, time);
}






