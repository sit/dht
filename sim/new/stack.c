#include "incl.h"
#include <stdlib.h>

Stack *newStackItem(int nodeId, void (*fun)()) 
{ 
  Stack *s;

  if (!(s = (Stack *)calloc(1, sizeof(Stack))))
    panic("newStackItem: memory alloc. error.\n");
  
  s->nodeId = nodeId;
  s->fun    = fun;

  return s;
}


Stack *pushStack(Stack *stack, Stack *s)
{
  s->next = stack;

  return s;
}

Stack *popStack(Stack *stack)
{
  Stack *s;

  if (!stack)
    return NULL;

  s = stack->next;
  
  free(stack);

  return s;
}


Stack *topStack(Stack *stack)
{
  return stack;
}


void freeStack(Stack *stack)
{
  if (stack)
    while (stack = popStack(stack));
}


/* use information at the top of stack to return */
void returnStack(int nodeId /* local node id */, Stack *stack)
{
  Stack *top = topStack(stack);
  Node *n    = getNode(nodeId);

  if (!top)
    return;

  if (!n) 
    freeStack(stack);
  else {
    if (top->nodeId == nodeId)
      top->fun(n, stack);
    else 
      genEvent(top->nodeId, top->fun, 
	       (void *)stack, Clock + intExp(AVG_PKT_DELAY));   
  }
}


void callStack(int nodeId, void (*fun)(), Stack *stack)
{
  Stack *top = topStack(stack);
  Node *n    = getNode(nodeId);

  if (!n) 
    freeStack(stack);
  else {
    if (top->nodeId == nodeId)
      fun(n, stack);
    else 
      genEvent(nodeId, fun, 
	       (void *)stack, Clock + intExp(AVG_PKT_DELAY));   
  }
}
