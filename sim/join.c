#include <stdlib.h>
#include "incl.h"

void join_entry(Node *n, Node *n1);
void join_cont1(Node *n, Stack *stack);
void join_cont2(Node *n, Stack *stack);
void join_return(Node *n, Stack *stack);

/* 
 * n joins the network;
 * nodeId is an arbitrary node in the network
 */ 
 
void join(Node *n, int *nodeId) {
  int i;
  Node *n1 = getNode(*nodeId);

  /* initialize all entries */
  for (i = 0; i < NUM_BITS; i++) 
    n->finger[i] = n->id;
  n->predecessor = n->successor = n->id;

  if (n1 && (*nodeId != -1)) 
    join_entry(n, n1);

  /* invoke stabilize */
  genEvent(n->id, stabilize_entry, (void *)NULL, 
	   Clock + unifRand(0.5*STABILIZE_PERIOD, 1.5*STABILIZE_PERIOD)); 
}


void join_entry(Node *n, Node *n1)
{
  Stack *stack;

  stack = pushStack(NULL, newStackItem(n->id, join_cont1));
  stack->data.key = n->id;
  CALL(n1->id, findPredecessor_entry, stack);
}


void join_cont1(Node *n, Stack *stack)
{
  Stack *top = topStack(stack);
  Node  *p   = getNode(top->ret.nid);

  if (!p) 
    KILL_REQ(stack);

  if ((p->successor == p->id) ||
      (between(n->id, p->id, p->successor, NUM_BITS) ||
       p->successor == n->id)) {

    updateSuccessor(n, p->successor);
    n->predecessor = p->id;
    top->fun = join_cont2;
    top->data.nid = p->successor;

    CALL(n->id, bootstrap_entry, stack);

  } else 
    CALL(p->id, getSuccessor, stack);
}


void join_cont2(Node *n, Stack *stack)
{
  Stack *top = topStack(stack);

  top->fun = join_return;
  top->data.nid = n->id;
  n->status = PRESENT;
  CALL(n->id, notify_entry, stack);
}

void join_return(Node *n, Stack *stack)
{
  updateDocList(n); 
  RETURN(n->id, stack = popStack(stack));
}



