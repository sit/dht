#include "stdlib.h"
#include "incl.h"

ID chooseX(Node *n);

// used for refreshing/updating nodes in the finger table.
// ask for the successor of a value x, and insert/refresh that
// successor (see processRequest) in the finger table

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
      // printf("ooo c=%f, n=%d, x=%d\n", Clock, n->id, x[i]);
      r = newRequest(x[i], REQ_TYPE_STABILIZE, REQ_STYLE_ITERATIVE, n->id);
      getNeighbors(n, r->x, &(r->pred), &(r->succ));
      
      dst = r->succ;
      if ((funifRand(0., 1.) < 0.1) && 
	  (n->fingerList->size <  MAX_NUM_FINGERS))
	dst = getRandomActiveNodeId();
      if (dst != n->id) {

	if (f = getFinger(n->fingerList, r->succ))
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


// choose value x for stabilization
ID chooseX(Node *n)
{
  ID      x;
  Finger *f;
  int     next;
#define PROB_FINGER    0.5

  // refresh a finger n+2^{i-1} with probability PROB_FINGER 
  if (funifRand(0., 1.) < PROB_FINGER) {
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
    if ((n->fingerList->size >= NUM_SUCCS) && funifRand(0., 1.) < 0.5) {
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

      

    
