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


#ifndef  INCL_REQUEST
#define INCL_REQUEST
 

// Request data structure
//  
// - a request r is resolved when it arrives at
//   node n, where n < r.x <= n.successor
// - after a request is resolved, it is sent back
//   to its initiator   

typedef struct request_ {
  ID  x;         // request value 
#define REQ_TYPE_STABILIZE   0
#define REQ_TYPE_INSERTDOC   1
#define REQ_TYPE_FINDDOC     2
#define REQ_TYPE_REPLACESUCC 3
#define REQ_TYPE_REPLACEPRED 4
#define REQ_TYPE_SETSUCC     5
#define REQ_TYPE_JOIN        6
  int type;      // request type
#define REQ_STYLE_ITERATIVE 0
#define REQ_STYLE_RECURSIVE 1
  int style;     // whether the request is reslved by using
                 // an iterative or recursive algorithm 
  ID  initiator; // node initiating the request
  ID  sender;    // sender of current request
  ID  pred;      // best known predecessor of x
  ID  succ;      // best known successor of x
  int done;      // TRUE after message has arrived at node n
                 // where n < r.x <= n.successor   
  // the following fields are used for retries
  ID  dst;
  ID  del;
  struct {
#define STACKNODE_SIZE 20
    ID nodes[STACKNODE_SIZE];
    int num;  // number of nodes in the stack
  } stack;
  int hops;
  int timeouts;

  struct request_ *next; 
} Request;
  
typedef struct requestList_ {
  Request *head;
  Request *tail;
} RequestList;

#endif // INCL_REQUEST







