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
#include <math.h>

#include "incl.h"

Node *NodeHashTable[HASH_SIZE];

// nodes are maintained in a closed hash data structure, i.e., 
// each entry in the hash points to a list of all nodes whose
// ids are hashed to that entry

Node *newNode(int id) 
{ 
  Node *n;

  if (!(n = (Node *)calloc(1, sizeof(Node))))
    panic("newNode: memory allocation error 1\n");
  if (!(n->fingerList = (FingerList *)calloc(1, sizeof(FingerList))))
    panic("newNode: memory allocation error 2\n");
  if (!(n->docList = (DocList *)calloc(1, sizeof(DocList))))
    panic("newNode: memory allocation error 2\n");
  if (!(n->reqList = (RequestList *)calloc(1, sizeof(RequestList))))
    panic("newNode: memory allocation error 3\n");
  n->id = id;

  return n;
}

void freeNode(Node *n) 
{ 
  free(n->docList);
  free(n->fingerList);
  free(n);
}


// add node n at the head of the list 
Node *addToList(Node *head, Node *n)
{
  n->next = head;
  return n;
}

Node *removeFromList(Node *head, Node *n)
{
  Node *n1;

  if (n == head)
    return n->next;

  for (n1 = head; ((n1->next != n) && n1); n1 = n1->next) 
    n1 = n1->next;

  if (!n1)
    panic("removeFromList: node not found\n");

  n1->next = n->next;
  return head;
}


void initNodeHashTable()
{
  int i;

  for (i = 0; i < HASH_SIZE; i++)
    NodeHashTable[i] = NULL;
}

Node *addNode(int id)
{
  Node *n = newNode(id);
  
  NodeHashTable[id % HASH_SIZE] = 
    addToList(NodeHashTable[id % HASH_SIZE], n);

  return n;
}

void deleteNode(Node *n)
{

  freeDocList(n);

  NodeHashTable[n->id % HASH_SIZE] = 
    removeFromList(NodeHashTable[n->id % HASH_SIZE], n);
  freeNode(n); 
}


Node *getNode(int id)
{
  Node *n = NodeHashTable[id % HASH_SIZE];

  if (id == -1)
    return NULL;

  for (; n; n = n->next)
    if (n->id == id)
      return n;

  return NULL;
}


int getRandomActiveNodeId()
{
  int i, idx;

  idx = unifRand(0, HASH_SIZE);

  for (i = idx; i < HASH_SIZE; i++)
    if (NodeHashTable[i] && NodeHashTable[i]->status == PRESENT)
      return (NodeHashTable[i])->id;
  
  for (i = idx; i; i--)
    if (NodeHashTable[i] && NodeHashTable[i]->status == PRESENT)
      return (NodeHashTable[i])->id;

  return -1;
}


// return a random node
int getRandomNodeId()
{
  int i;

  // check whether there hash  is empty -- probably we should just 
  // maintain # of nodes in the network for easy check...
  for (i = 0; i < HASH_SIZE; i++)
    if (NodeHashTable[i])
      break;

  if (i == HASH_SIZE)
    // hash empty 
    return -1;

  while (1) {
    i = unifRand(0, HASH_SIZE - 1);
    if (NodeHashTable[i])
      return (NodeHashTable[i])->id;
  }
  return -1;
}


ID popNode(Request *r)
{
  if (r->stack.num == 0)
    return -1;
  else {
    r->stack.num--;
    return r->stack.nodes[r->stack.num];
  }
}


void pushNode(Request *r, ID nid)
{
  if ((r->stack.num >= STACKNODE_SIZE) || 
      (r->stack.nodes[r->stack.num - 1] == nid))
    return;
  r->stack.nodes[r->stack.num] = nid;
  r->stack.num++;
}


// each node fails independently with probability p
void netFailure(Node *n, int *percentage)
{
  double p = *percentage / 100.;
  int    i;

  for (i = 0; i < HASH_SIZE; i++) {
    for (n = NodeHashTable[i]; n; n = n->next)
      if (n->status == PRESENT && unifRand(0., 1.) < p)
	nodeFailure(n, NULL);
  }
}


void printNodeInfo(Node *n)
{
  if (n->status == ABSENT)
    return;

#ifdef TRACE
  if (getSuccessor(n) == n->id)
    // node is diconnected
    printf("o");
#endif

  printf("Node = %d (%d)\n", n->id, n->status);

  printFingerList(n);
  printDocList(n);
  printReqList(n);
}

void printAllNodesInfo()
{
  int i;
  Node *n;

  printf("---------\n");

  for (i = 0; i < HASH_SIZE; i++) {
    for (n = NodeHashTable[i]; n; n = n->next)
      printNodeInfo(n);
  }
}


