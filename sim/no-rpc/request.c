#include <stdlib.h>
#include <stdio.h>

#include "incl.h"

Request *newRequest(ID x, int type, int style, ID initiator)
{
  Request *r;

  if (!(r = (Request *)calloc(1, sizeof(Request))))
    panic("newRequest: memory allocation error\n");

  r->x = x;
  r->type = type;
  r->style = style;
  r->initiator = r->succ = r->pred = r->sender = initiator;

  return r;

}

// add request at the tail of the pending request list
void insertRequest(Node *n, Request *r)
{ 
  RequestList *rList = n->reqList;

  /*
printf("==> Clock=%f req:(x=%d, type=%d, style=%d, i=%d, s=%d, p=%d, s=%d)\n",
       Clock, r->x, r->type, r->style, r->initiator, r->sender,
       r->pred, r->succ);
  */

  if (!rList->head) 
    rList->head = rList->tail = r;
  else {
    rList->tail->next = r;
    rList->tail = r;
  }
}


// get request at the head of the pending request list
Request *getRequest(Node *n)
{
  RequestList *rList = n->reqList;
  Request     *r;

  if (!rList->head)
    return NULL;

  r = rList->head;
  rList->head = rList->head->next;
  if (!rList->head) 
    rList->tail = NULL;

  r->next = NULL; 
  return r;
}


// process request
void processRequest(Node *n)
{
  ID      succ, pred, dst, old_succ;
  Request *r = getRequest(n);
  
  // printf("llll C=%f, n=%d\n", Clock, n->id);  
  // periodicall invoke process request
  if (n->status == PRESENT)
    genEvent(n->id, processRequest, (void *)NULL, 
	     Clock + unifRand(0.5*PROC_REQ_PERIOD, 1.5*PROC_REQ_PERIOD));
  if (!r)
    return;

  // update/refresh finger table
  insertFinger(n, r->initiator);
  if (r->initiator != r->sender)
    insertFinger(n, r->initiator);

  // consult finger table to get the best predecessor/successor 
  // of r.x  
  getNeighbors(n, r->x, &pred, &succ);

  // update r's predecessor/successor
  if (between(pred, r->pred, r->x, NUM_BITS))
      r->pred = pred;
  if (between(succ, r->x, r->succ, NUM_BITS) || (r->x == succ))
      r->succ = succ;

  if (between(r->x, n->id, getSuccessor(n), NUM_BITS) || 
      (getSuccessor(n) == r->x)) 
    // r->x's successor is n's successor
    r->done = TRUE;

  r->sender = n->id;

  if (between(r->x, getPredecessor(n), n->id, NUM_BITS) || (r->x == n->id)) {
    switch (r->type) {
    case REQ_TYPE_INSERTDOC:
      insertDocumentLocal(n, &r->x);
      free(r);
      return;
    case REQ_TYPE_FINDDOC:
      findDocumentLocal(n, &r->x);
      free(r);
      return;
    }
  }

  if (r->done) {
    // is n the request's initiator?
    if (r->initiator == n->id) {
      switch (r->type) {
      case REQ_TYPE_STABILIZE:
	// if n's successor changes,
        // iterate to get a better successor
	old_succ = getSuccessor(n);
	insertFinger(n, r->succ);
	if (old_succ == getSuccessor(n)) {
	  free(r);
	  return;
	} else {
	  dst = r->succ;
	  r->done = FALSE;
	  break;
	}
      case REQ_TYPE_PRED:
	insertFinger(n, r->succ);
	free(r);
	return;
      case REQ_TYPE_INSERTDOC:
      case REQ_TYPE_FINDDOC:
	dst = r->succ;
	break;
      default:
	return;
      }
	  
    } else
      // send request back to intiator
      dst = r->initiator;
  } else {
    if (r->initiator == n->id)
      // send to the closest predecessor
      dst = r->pred;
    else {
      if (r->style == REQ_STYLE_ITERATIVE)
	dst = r->initiator;
      else
	dst = r->pred;
    }
  }

  // send message to dst
  genEvent(dst, insertRequest, (void *)r, Clock + intExp(AVG_PKT_DELAY));
}  


void printReqList(Node *n)
{
  Request *r = n->reqList->head;

  printf("   request list:");
  
  for (; r; r = r->next) {
    printf(" %d/%d/%d/%d", r->x, r->type, r->succ, r->pred);
    if (r->next)
      printf(",");
  }

  if (n->reqList->head)
    printf(" (h=%d, t=%d)", n->reqList->head->x, n->reqList->tail->x); 

  printf("\n");
}


