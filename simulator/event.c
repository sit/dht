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
#include <string.h>
#include "incl.h"

// use a calendar queue to maintain events
Heap *initEventQueue()
{
  Heap* h;
  int i;

  if (!(h = (Heap *)calloc(1, MAX_NUM_ENTRIES*sizeof(Heap))))
    panic("initEventQueue: memory alloc. error.\n");

  for (i = 0; i < MAX_NUM_ENTRIES; i++) 
     initHeap(&h[i], 1500);

  return h;
}

Event *newEvent(int nodeId, void (*fun)(), void *params)
{
  Event *ev;

  if (!(ev = (Event *)calloc(1, sizeof(Event))))
    panic("newEvent: memory alloc. error.\n");

  ev->nodeId = nodeId;
  ev->fun = fun;
  ev->params = params;

  return ev;
}


// insert new event in the calendar queue 
void insertEvent(CalQueue *evCal, Event *ev, double time) 
{
  int idx;

  if (time > MAX_TIME)
    return;

  if (evCal->time > time)
    panic("InsertEvent: event inserted in the past\n");

  // get calendar queue entry to insert the new event 
  idx = ((int)time/ENTRY_TUNIT) % evCal->size;

  if (idx >= evCal->size)
    panic("insertEvent: exceed calendar queue size\n");

  insertHeapNode(&evCal->heaps[idx], time, ev);
}


// return next event from the calendar queue
Event *getEvent(CalQueue *evCal, double time, double* event_time)
{
  Event *p;
  int slice_begin = ((int)time/ENTRY_TUNIT)*ENTRY_TUNIT;
  int idx = ((int)time/ENTRY_TUNIT) % evCal->size;
  static int oldIdx = 0;

  if (idx - oldIdx > 1) 
    panic("getEvent: step over an entry!\n");
  else
    oldIdx = idx;

  evCal->time = time;

  if ( !(p = inspectHeapMax(&evCal->heaps[idx], event_time)) )
    return NULL;

  // check to make sure we're in the correct timeslice
  if (*event_time < slice_begin) {
      printf("time: %f. p->time: %f. slice_begin: %d\n", 
	    time, *event_time, slice_begin);
      panic("");
  } else if (*event_time >= slice_begin + ENTRY_TUNIT) {

      // corresponds to the future
      return NULL; 

  }

  p = extractHeapMax(&evCal->heaps[idx], event_time);

  return p;
}     


// create a new event and insert it in the calendar queue
void genEvent(int nodeId, void (*fun)(), void *params, double time)
{
  Event *ev;

  ev = newEvent(nodeId, fun, params);

  insertEvent(&EventQueue, ev, time);
}

///////////////////////////////////////////////////////////////////
//                  Heap functions                               //
//////////////////////////////////////////////////////////////////

// stupid heap implementation. use arrays and realloc when necessary.

// allocate room for 1500 events at a time
#define REALLOC_INCREMENT 1500

#define parent(i) (i/2)
#define left(i)   (2*i)
#define right(i)  (2*i + 1)


void initHeap(Heap* h, int num)
{
    memset(h, 0, sizeof(Heap)); 

    // we'll index our array from 1
    if (!(h->array = (HeapNode*)calloc(num + 1, sizeof(HeapNode))))
	panic("newHeap: memory alloc error. 2.\n");

    h->size = 0;
    h->max  = num;
    
}

void insertHeapNode(Heap* h, double time, Event* ev)
{
    int idx;

    HeapNode hn;
    hn.time = time;
    hn.eventp = ev;

    h->size++;
    if (h->size > h->max) {

	h->array = realloc(h->array,
		          (h->max+1)*sizeof(HeapNode) + // what we have now
			   REALLOC_INCREMENT*sizeof(HeapNode));
	h->max += REALLOC_INCREMENT;

    }

    idx = h->size;

    while (idx > 1 && h->array[parent(idx)].time > time) {

	h->array[idx] = h->array[parent(idx)];
	idx = parent(idx);

    }

    h->array[idx] = hn;
}

Event* extractHeapMax(Heap* h, double* time)
{
    void *ret;
    if (h->size < 1) 
	return NULL;

    ret = h->array[1].eventp; 
    *time = h->array[1].time;

    h->array[1] = h->array[h->size];
    h->size--;
    
    heapify(h, 1); // the 1-indexed array again

    return ret;
}

Event* inspectHeapMax(Heap* h, double* time)
{
    void *ret;
    if (h->size < 1)
	return NULL;

    *time = h->array[1].time;
    ret = h->array[1].eventp;
    return ret;
}

void heapify(Heap* h, int idx)
{
    int i = idx;
    while (1) {

	int l = left(i);
	int r = right(i);
	int smallest = i;

	if (l <= h->size && h->array[l].time < h->array[i].time)
	    smallest = l;

	if (r <= h->size && h->array[r].time < h->array[smallest].time)
	    smallest = r;

	if (smallest != i) {

	    HeapNode temp = h->array[smallest];
	    h->array[smallest] = h->array[i];
	    h->array[i] = temp;

	    i = smallest;

	} else 

	    break;

    }
}


