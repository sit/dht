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
#include "stdlib.h"
#include "incl.h"

ID chooseX(Node *n);

// refresh/update nodes in the finger table.
// each iteration refresh node's successor.
// in addition, a pseudo-random value is generated
// and the successor of x is inserted (or refreshed)
// in the finger table. x is chosen such that
// the NUM_SUCC immediate successors of n and its proper 
// fingers (i.e., a proper finger is the successor of 
// n+2^{i-1}) are refreshed with a high probability

void stabilize(Node *n)
{
  Request *r;
  Finger  *f;
  ID       x[2], dst;
  int      i;

  if (n->status == ABSENT)
    return;


  if (n->fingerList->head) {

    x[0] = chooseX(n);
    x[1] = successorId(n->id, 1);

    i = (x[0] == x[1] ? 1 : 0);    
      
    for (; i < 2; i++) {
      r = newRequest(x[i], REQ_TYPE_STABILIZE, REQ_STYLE_ITERATIVE, n->id);

      getNeighbors(n, r->x, &(r->pred), &(r->succ));
      
      dst = r->succ; 
      if ((funifRand(0., 1.) < 0.1) && 
	  (n->fingerList->size <  MAX_NUM_FINGERS))
	dst = getRandomActiveNodeId();
      if (dst != n->id) {

	if ((f = getFinger(n->fingerList, r->succ)))
          // set timeout; if n does not hear back
          // from f within 4*PROC_REQ_PERIOD,
          // f is removed from the finger list 
          // upon invoking cleanFingerList() 
	  f->expire = Clock + 4*PROC_REQ_PERIOD;

	// insert request at r.x's successor
	genEvent(dst, insertRequest, (void *)r, 
	       Clock + intExp(AVG_PKT_DELAY));
      }
    }
  }

  genEvent(n->id, stabilize, (void *)NULL, 
	   Clock + unifRand(0.5*STABILIZE_PERIOD, 1.5*STABILIZE_PERIOD));
}



// choose a pseudo-random value x;
// stabilize() uses x to update/refresh successor(x)
//
// x is choosen such that a proper finger is refreshed
// with probability PROB_REFRESH_FINGER, and an immediate
// successor is refreshed with probability 
// (1 - PROB_REFRESH_FINGER)*PROB_REFRESH_SUCC
//
#define PROB_REFRESH_FINGER    0.5
#define PROB_REFRESH_SUCC      1./*0.5*/

ID chooseX(Node *n)
{
  ID      x;
  Finger *f;
  int     next;

  // refresh a finger n+2^{i-1} with probability PROB_REFRESH_FINGER 
  if (funifRand(0., 1.) < PROB_REFRESH_FINGER) {
    if (n->fingerId >= NUM_BITS)
      n->fingerId = 1;
    x = successorId(n->id, (1 << n->fingerId));
    n->fingerId++;
    return x;
  } else {
    // refresh an arbitrary finger in the list
    if (!n->fingerList->head)
      panic("chooseX: fingerList empty!\n");

    next = 0;
#define PROB_REFRESH_
    if ((n->fingerList->size >= NUM_SUCCS) && 
	funifRand(0., 1.) <= PROB_REFRESH_SUCC) {
      next = unifRand(0, 2);
      x = unifRand(0, NUM_SUCCS);
    } else
      x = unifRand(0, n->fingerList->size);
    for (f = n->fingerList->head; (f && x--) ; f = f->next);
    if (!f)
      panic("chooseX: end of fingerList reached!\n");
    return successorId(f->id, next);
  }
}

      

    
