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

#ifndef INCL_EVENTS
#define INCL_EVENTS

// these objects get stored in HeapNodes
typedef struct _event {
  int           nodeId;    /* nodeId */
  void          (*fun)();  /* function to be called when the event occurs */
  void           *params;  /* address of the parameters to be passed to fun */
} Event;


// a (key,value) pair, where the key is the time the event will take place
// and the value is a pointer to a HeapNode
typedef struct _heapnode {
    double time;
    Event *eventp;	
} HeapNode;

typedef struct _Heap {
    int size;	     // number of events in the heap
    int max;	     // maximum number of events in the heap
    HeapNode* array; // array of HeapNodes
} Heap;

typedef struct _calQueue {
  int     size;   /* size of the calendar queueue */
  double  time;   /* current time */
  struct _Heap* heaps; /* array of heaps */
} CalQueue;


#endif
