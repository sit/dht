#include <stdlib.h>
#include <stdio.h>

#include "incl.h"
int processRequest1(Node *n, Request *r);

void insertRequest_wait(Node *n, Request *r);

// #define PREFIX_MATCH

Request *newRequest(ID x, int type, int style, ID initiator)
{
  Request *r;

  if (!(r = (Request *)calloc(1, sizeof(Request))))
    panic("newRequest: memory allocation error\n");

  r->x = x;
  r->type = type;
  r->style = style;
  r->initiator = r->succ = r->pred = r->sender = initiator;

  r->del = -1;
  return r;

}

// add request at the tail of the pending request list
void insertRequest(Node *n, Request *r)
{ 
  RequestList *rList = n->reqList;

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
  Request *r;

  // periodicall invoke process request
  if (n->status == PRESENT)
    genEvent(n->id, processRequest, (void *)NULL, 
	     Clock + unifRand(0.5*PROC_REQ_PERIOD, 1.5*PROC_REQ_PERIOD));

  while (r  = getRequest(n)) {

    if (r->del != -1) {
      Finger *f = getFinger(n->fingerList, r->del);
      if (f) removeFinger(n->fingerList, f);
      r->del = -1;
    }

    // update/refresh finger table
    insertFinger(n, r->initiator);
    if (r->initiator != r->sender)
      insertFinger(n, r->sender);

    // process insert document, find document, and several
    // optimization requests 
    if (processRequest1(n, r)) {
      free(r);
      continue;
    }

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
	    continue;
	  } else {
	    dst = r->succ;
	    r->done = FALSE;
	    break;
	  }
	case REQ_TYPE_INSERTDOC:
	case REQ_TYPE_FINDDOC:
          // forward the request to the node responsible
          // for the document r->x; the processing of this request
          // takes place in processRequest1 
#ifdef PREFIX_MATCH
	  if (prefixLen(r->x, r->succ) > prefixLen(r->x, r->pred))
	    dst = r->succ;
	  else 
	    dst = r->pred;
#else
	  dst = r->succ;  
#endif
	  break;
	default:
	  continue;
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
    if (n->id == r->initiator) {
      r->dst = dst;
      genEvent(n->id, insertRequest_wait, (void *)r, 
	       Clock + intExp(AVG_PKT_DELAY));
    } else {
      genEvent(dst, insertRequest, (void *)r, Clock + intExp(AVG_PKT_DELAY));
    }
  }
}  

int processRequest1(Node *n, Request *r)
{
#ifdef OPTIMIZATION
  switch (r->type) {
  case REQ_TYPE_REPLACESUCC:
    if (r->sender == getSuccessor(n)) {
      removeFinger(n->fingerList, n->fingerList->head);
      insertFinger(n, r->x);
    }
    return TRUE;
  case REQ_TYPE_REPLACEPRED:
    if (r->sender == getPredecessor(n)) {
      removeFinger(n->fingerList, n->fingerList->tail);
      insertFinger(n, r->x);
    }
    return TRUE;
  case REQ_TYPE_SETSUCC:
    insertFinger(n, r->x);
    return TRUE;
  }
#endif // OPTIMIZATION

  if  (between(r->x, getPredecessor(n), n->id, NUM_BITS) || (r->x == n->id)) {
    switch (r->type) {
    case REQ_TYPE_INSERTDOC:
      insertDocumentLocal(n, &r->x);
      return TRUE;
    case REQ_TYPE_FINDDOC:
      findDocumentLocal(n, &r->x);
      return TRUE;
    }
  }
  return FALSE;
}


// add request at the tail of the pending request list
void insertRequest_wait(Node *n, Request *r)
{
  // send message to dst
  if (getNode(r->dst)->status != PRESENT) {
    // printf("NOT_PRESENT=%d, %f\n", r->dst, Clock);
    r->done = FALSE;
    r->succ = r->pred = r->initiator;
    r->del = r->dst; // delete node
    if ((r->dst = popNode(r)) == -1)
      genEvent(r->initiator, insertRequest, (void *)r, Clock + TIME_OUT);
    else 
      genEvent(r->dst, insertRequest, (void *)r, Clock + TIME_OUT);
  }
  else {
    pushNode(r, r->dst);
    genEvent(r->dst, insertRequest, (void *)r, Clock);
    // printf("PRESENT\n");
  }
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






