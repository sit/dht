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

void nodeFailure(Node *n, void *dummy);

void leave(Node *n, void *dummy) {
  Request *r;
  Finger  *f;

  // move documents to the first finger 
  for (f = n->fingerList->head; f; f = f->next) {
    Node *n1 = getNode(f->id);

    if (n1->status == PRESENT) {
      moveDocList(n, n1);
      break;
    }
  }

#ifdef OPTIMIZATION
  // announce predecessor about the new successor
  r = newRequest(getSuccessor(n), REQ_TYPE_REPLACESUCC, 
		 REQ_STYLE_ITERATIVE, n->id);
  genEvent(getPredecessor(n), insertRequest, (void *)r, 
	   Clock + intExp(AVG_PKT_DELAY));
  // announce successor about the new predecessor
  r = newRequest(getPredecessor(n), REQ_TYPE_REPLACEPRED, 
		 REQ_STYLE_ITERATIVE, n->id);
  genEvent(getSuccessor(n), insertRequest, (void *)r, 
	   Clock + intExp(AVG_PKT_DELAY));
#endif // OPTIMIZATION

  n->status = ABSENT;
}


void nodeFailure(Node *n, void *dummy) {
  Finger *f;

  // XXX move documents because in our experiments we look only
  // for routing failures; if a document is not found, then  
  // this is only due to routing failures or routing 
  // inconsistencies
  //
  // should replicate documents in the future...
  for (f = n->fingerList->head; f; f = f->next) {
    Node *n1 = getNode(f->id);

    if (n1->status == PRESENT) {
      moveDocList(n, n1);
      n->status = ABSENT;
      return;
    }
  }

  printf("No finger alive for node %d at %f\n", n->id, Clock);
}






