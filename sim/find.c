#include "incl.h"

void findSuccessor_return(Node *n, Stack *stack);

void findPredecessor_return(Node *n, void *stack);
void findPredecessor_remote(Node *n, void *stack);



/*******************************************************
 *       Find successor
 *
 *      findSuccessor_entry
 *             |
 *             |
 *             v
 *      findPredecessor_entry
 *             |  
 *             |  
 *             v  
 *      findSuccessor_return
 *
 *******************************************************/


void findSuccessor_entry(Node *n, Stack *stack)
{
  Stack *top = topStack(stack);

  stack = pushStack(stack, newStackItem(n->id, findSuccessor_return));
  topStack(stack)->data.key = top->data.key;
  CALL(n->id, findPredecessor_entry, stack);
}


void findSuccessor_return(Node *n, Stack *stack)
{
  Stack *top = topStack(stack);

  if (top->data.key == top->ret.nid)
    top->next->ret.nid = top->ret.nid;
  else 
    top->next->ret.nid = top->ret.succ;
  RETURN(n->id, stack = popStack(stack));
}



/*******************************************************
 *       Find predecessor
 *
 *      findPredecessor_entry
 *             |
 *             |
 *             v
 *      findPredecessor_remote
 *             |  ^
 *             |  |
 *             v  |
 *      findPredecessor_return
 *
 *******************************************************/

/* start looking for key at node n */
void findPredecessor_entry(Node *n, Stack *stack)
{
  Stack *top =  topStack(stack);
  int key = top->data.key;

  if ((n->id == n->successor) || 
       between(key, n->id, n->successor, NUM_BITS) || 
       (key == n->id)) {
    top->ret.nid = n->id;
    top->ret.succ = n->successor;
    top->ret.pred = n->predecessor;
    top->ret.found = 1;
    RETURN(n->id, stack);
  } else {
    stack = pushStack(stack, newStackItem(n->id, findPredecessor_return));
    topStack(stack)->data.key = top->data.key;
    CALL(closestPreceedingFinger(n, key), findPredecessor_remote, stack);
  }
}


void findPredecessor_return(Node *n, void *stack)
{
  Stack *top = topStack(stack);
  
  if (top->ret.found) {
    top->next->ret.nid = top->ret.nid;
    top->next->ret.succ = top->ret.succ;
    top->next->ret.pred = top->ret.pred;
    RETURN(n->id, stack = popStack(stack));
  } else 
    CALL(top->ret.nid, findPredecessor_remote, stack);
}
    

void findPredecessor_remote(Node *n, void *stack)
{
  Stack *top = topStack(stack);
  int key = top->data.key;
  
  if (between(key, n->id, n->successor, NUM_BITS) || 
      (key == n->id)) {
    top->ret.nid = n->id;
    top->ret.succ = n->successor;
    top->ret.pred = n->predecessor;
    top->ret.found   = TRUE;

  } else {
    top->ret.nid = closestPreceedingFinger(n, key);
    top->ret.found   = FALSE;
  }
  RETURN(n->id, stack);
}


/*******************************************************/
/*      Return closest preceeding finger
/*******************************************************/


/* return closest finger preceeding id */
int closestPreceedingFinger(Node *n, int id)
{
  int i;
  
  if (id == n->id)
    return n->id;

  for (i = NUM_BITS - 1; i >= 0; i--)
    if (between(n->finger[i], n->id, id, NUM_BITS) ||
	(n->finger[i] == id))
      return n->finger[i];

  return n->id;
}




void getSuccessor(Node *n, void *stack)
{
  Stack *top = topStack(stack);

  top->ret.nid = n->successor;
  RETURN(n->id, stack);
}


void getPredecessor(Node *n, void *stack)
{
  Stack *top = topStack(stack);

  top->ret.nid = n->predecessor;

  RETURN(n->id, stack);
}


