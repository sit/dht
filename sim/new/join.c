#include <stdlib.h>
#include "incl.h"

void join_entry(Node *n, Node *n1);
void join_exit(Node *n, Stack *stack);

/* 
 * n joins the network;
 * nodeId is an arbitrary node in the network
 */ 
 
void join(Node *n, int *nodeId) {
  int i;
  Node *n1;

  if (!(n1 = getNode(*nodeId)))
    // probably nodeId has been deleted in the meantime
    n1 = getNode(getRandomActiveNodeId());

    
  /* initialize all entries */
  for (i = 0; i < NUM_BITS; i++) 
    n->finger[i] = n->id;
  n->predecessor = n->successor = n->id;

  if (n1 && (*nodeId != -1)) 
    join_entry(n, n1);
  else {
    n->status = PRESENT;
    stabilize(n);
    fixFingers(n);
  }
}


void join_entry(Node *n, Node *n1)
{
  Stack *stack;

  stack = pushStack(NULL, newStackItem(n->id, join_exit));
  stack->data.key = n->id;
  n->status = PRESENT;
  CALL(n1->id, findPredecessor_entry, stack);

}


void join_exit(Node *n, Stack *stack)
{
  Stack *top = topStack(stack);
  Node  *p   = getNode(top->ret.nid);

  /* start periodic stabilization */
  stabilize(n);
  fixFingers(n);

  if (!p) 
    KILL_REQ(stack);

  updateNodeState(n, p->successor);

  RETURN(n->id, stack = popStack(stack));
}


