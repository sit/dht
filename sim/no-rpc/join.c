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
#include "incl.h"

// initiate join request
void join(Node *n, int *nodeId) {
  Node *n1;

  if ((!(n1 = getNode(*nodeId))) || (n1 && n->status == ABSENT))
    // probably nodeId has been deleted in the meantime
    n1 = getNode(getRandomActiveNodeId());

  if (n1) {
    Request *r;
    
    // ask n1 for the successor of new node n
    r = newRequest(n->id, REQ_TYPE_JOIN, REQ_STYLE_ITERATIVE, n1->id);
    insertRequest(n1, r);
  } else {

    // this should be the first node in teh network 
    n->status = PRESENT;    
    stabilize(n);
    processRequest(n);
  }
}


// complete the join operation. 
// this function is invoked after the successor
// of the node joining the system is found
 
void join1(ID id, ID succ) 
{
  Node *n = getNode(id);
  Node *s = getNode(succ);

  if (s->status == PRESENT) {
    insertFinger(n, s->id);
    n->status = TO_JOIN;
    stabilize(n);
    processRequest(n);
  } else
    printf("%f join %d failure\n", Clock, n->id);
}

