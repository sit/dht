#include <stdlib.h>

#include "incl.h"

void stabilize_loop(Node *n, Stack *stack);
void stabilize_succ(Node *n, Stack *stack);
void stabilize_pred(Node *n, Stack *stack);


/*******************************************************/
/*       stabilize (always called by local node)
/*******************************************************/
  
void stabilize(Node *n)
{
  notify_entry(n, NULL);
}

void stabilize_entry(Node *n, Stack *stack)
{
  Stack *top = topStack(stack);

  if (n->status != PRESENT) {
    Node *n1 = getNode(getRandomNodeId());
    if (n1 && n1->status == PRESENT)
      join_entry(n, n1);
    return;
  }

  stack = pushStack(stack, newStackItem(n->id, stabilize_loop));
  topStack(stack)->data.i = 0;
  CALL(n->id, stabilize_loop, stack);
}

void stabilize_loop(Node *n, Stack *stack)
{
  Stack *top = topStack(stack);
  int      i = top->data.i;

  /*
  for (; i < NUM_BITS; i++)
    if (n->finger[i] != n->id)
      break;
  */
  if (i < NUM_BITS) {
    top->data.i = i;
    top->fun = stabilize_succ;
    CALL(n->finger[i], getPredecessor, stack);
  } else {
    top->fun = stabilize_pred;
    CALL(n->predecessor, getSuccessor, stack);
  }
}


void stabilize_succ(Node *n, Stack *stack)
{ 
  Stack *top = topStack(stack);
  int     id = top->ret.nid;
  int      i = top->data.i;

  if (id == fingerStart(n, i) || 
      between(id, fingerStart(n, i), n->finger[i], NUM_BITS)) {
    setFinger(n, i, id);
  }
  (top->data.i)++;
  CALL(n->id, stabilize_loop, stack);
}

void stabilize_pred(Node *n, Stack *stack)
{ 
  Stack *top = topStack(stack);
  int     id = top->ret.nid;
 
  if (between(id, n->predecessor, n->id, NUM_BITS)) 
    n->predecessor = id;
  top->fun = stabilize_pred;
  top->data.nid  = n->id;

  /* invoke stabilize */
  if (n->status == PRESENT)
    genEvent(n->id, stabilize_entry, (void *)NULL, 
	     Clock + unifRand(0.5*STABILIZE_PERIOD, 1.5*STABILIZE_PERIOD));

  updateDocList(n);
  RETURN(n->id, stack = popStack(stack));
}

