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

#include <stdlib.h>
#include <stdio.h>

#include "incl.h"
int processRequest1(Node *n, Request *r);

void insertRequest_wait(Node *n, Request *r);

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

  // periodically invoke process request
  if (n->status != ABSENT)
    genEvent(n->id, processRequest, (void *)NULL, 
	     Clock + unifRand(0.5*PROC_REQ_PERIOD, 1.5*PROC_REQ_PERIOD));

  while ((r  = getRequest(n))) {

    if (r->del != -1) {
      Finger *f = getFinger(n->fingerList, r->del);
      if (f) removeFinger(n->fingerList, f);
      r->del = -1;
    }

    // update # of hops
    if (r->initiator != n->id)
      r->hops++;

    // update/refresh finger table
    insertFinger(n, r->initiator);
    if (r->initiator != r->sender)
      insertFinger(n, r->sender);

    // process: insert document, find document, and several
    // optimization requests 
    if (processRequest1(n, r)) {
      free(r);
      continue;
    }

    // consult finger table to get the best predecessor/successor 
    // of r.x  
    getNeighbors(n, r->x, &pred, &succ);
    
    // update r's predecessor/successor
    if (between(pred, r->pred, r->x))
      r->pred = pred;
    if (between(succ, r->x, r->succ) || (r->x == succ))
      r->succ = succ;
    
    if (between(r->x, n->id, getSuccessor(n)) || 
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

	  // update old successor, just in case r->succ will replace it
          // otherwise the old successor timeouts and it is evicted
	  insertFinger(n, n->fingerList->head->id); 

	  // the following test is a little bit of cheating; this 
          // approximates an implementation in which n checks
          // r->succ right away to see whether it is up
	  if (getNode(r->succ)->status != ABSENT)
	    insertFinger(n, r->succ);
	  if (old_succ == getSuccessor(n)) {
	    free(r);
	    if (n->status == TO_JOIN) 
	      copySuccessorFingers(n);
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
	  dst = r->succ;  
	  break;
	default:
	  continue;
	}
      } else
	// send request back to initiator
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
      // fake transmission to dst, without leaving
      // node n (i.e., the request's initiator).
      // this is used to implement lookup retries: 
      // if dst is alive when the request is processed,
      // the request is forwarded immediately to dst;
      // otherwise, the request is forwarded to the previous
      // node visited by the request 
      genEvent(n->id, insertRequest_wait, (void *)r, 
	       Clock + intExp(AVG_PKT_DELAY));
    } else {
      genEvent(dst, insertRequest, (void *)r, Clock + intExp(AVG_PKT_DELAY));
    }
  }
}  


// resolve request locally
int processRequest1(Node *n, Request *r)
{
  Node *s;

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

  // process insert/find document and node join requests
  if  ((between(r->x, n->id, getSuccessor(n)) || 
       (r->x == getSuccessor(n))) || 
	((r->type == REQ_TYPE_JOIN && 
	  r->initiator == r->pred && r->pred == r->succ))) {
    switch (r->type) {

    case REQ_TYPE_INSERTDOC:
      printf("insert doc request (hops= %d timeouts= %d ); ", 
	     r->hops, r->timeouts);
      insertDocumentLocal(getNode(getSuccessor(n)), &r->x);
      return TRUE;

    case REQ_TYPE_FINDDOC:
      printf("find doc request (hops= %d timeouts= %d ); ", 
	     r->hops, r->timeouts);
      s = getNode(getSuccessor(n));
      findDocumentLocal(s, &r->x);
      if (s->status == ABSENT)
	removeFinger(n->fingerList, n->fingerList->head);
      return TRUE;

    case REQ_TYPE_JOIN:
      join1(r->x, getSuccessor(n));
      return TRUE;
    }
  }
  return FALSE;
}


// used to implement backtracking when the next hop 
// (r->dst) along the request r's path fails.
// if r->dst is alive, r is forwarded immediately 
// to r->dst; otherwise, r->dst is forwarded to the 
// previous node visited by the request 
// 
// NOTE: This works only for ITERATIVE requests

void insertRequest_wait(Node *n, Request *r)
{

  // send message to dst
  if (getNode(r->dst)->status != PRESENT) {

    r->done = FALSE;
    getNeighbors(n, r->x, &(r->pred), &(r->succ));
    r->del = r->dst; // delete node
    r->timeouts++;    // increments the number of timeouts 
    if ((r->dst = popNode(r)) == -1) {
      // no node to retry; restart the request
      if (getNode(r->initiator) && 
	  (getNode(r->initiator)->status != PRESENT)) 
	printf("request of type %d has failed at time %f: initiator node %d has left the system\n",
	       r->type, Clock, r->initiator);
      genEvent(r->initiator, insertRequest, (void *)r, Clock + TIME_OUT);
    } else 
      // next hop has failed; forward request to the 
      // previous node visited by the request
      genEvent(r->dst, insertRequest, (void *)r, Clock + TIME_OUT);

  } else {
    pushNode(r, r->dst);
    genEvent(r->dst, insertRequest, (void *)r, Clock);
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


// get successor's finger table;
// this function is called when a 
// node joins the system

void copySuccessorFingers(Node *n)
{
  Finger *f;
  int    i;

  f = n->fingerList->head;
  for (i = 0; i < NUM_SUCCS; i++) {
    if (!f)
      break;
    insertFinger(n, f->id);
    f = f->next;
  }
}

      
    
  


