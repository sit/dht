#include <stdlib.h>

#include "incl.h"


void stabilize_entry(Node *n, Stack *stack);
void stabilize_exit(Node *n, Stack *stack);
void notify(Node *n, Stack *stack);
void getNodeInfo(Node *n, Stack *stack);
void fixFingers_entry(Node *n, Stack *stack);
void fixFingers_exit(Node *n, Stack *stack);

// stabilize: periodically verify n's immediate successor
  
void stabilize(Node *n)
{
  if (n->status == PRESENT)
    genEvent(n->id, stabilize_entry, (void *)NULL, 
	     Clock + unifRand(0.5*STABILIZE_PERIOD, 1.5*STABILIZE_PERIOD));
}


void stabilize_entry(Node *n, Stack *stack)
{

  if (n->status != PRESENT) {
    KILL_REQ(stack);
    return;
  }

  genEvent(n->id, stabilize_entry, (void *)NULL, 
	   Clock + unifRand(0.5*STABILIZE_PERIOD, 1.5*STABILIZE_PERIOD));

  if (n->successor == n->predecessor && n->successor == n->id) {
    printf("=======> %d\n", n->id);
    KILL_REQ(stack);
    return;
  }

  if (n->successor != n->id) {
    stack = pushStack(stack, newStackItem(n->id, stabilize_exit));
    if (getNode(n->successor)) {
      CALL(n->successor, getNodeInfo, stack);
      return;
    } else 
      deleteRefFromTables(n, n->successor);
  }    
  RETURN(n->id, stack);  
}

void stabilize_exit(Node *n, Stack *stack)
{
  Stack *top = topStack(stack);
  int     id = top->ret.nid;
  Node  *succ = getNode(id);

  if (!succ)
    deleteRefFromTables(n, id);
  else {
    if (succ->status == TO_LEAVE) {
      deleteRefFromTables(n, id);
      if (between(top->ret.pred,  n->id, n->successor, NUM_BITS)) 
	updateNodeState(n, top->ret.pred);
      else
	updateNodeState(n, top->ret.succ);
    } else {
      if (between(top->ret.pred, n->id, n->successor, NUM_BITS))  
	updateNodeState(n, top->ret.pred);
    }
  }

  // notify successor
  CALL(n->successor, notify, newStackItem(n->id, NULL));

  RETURN(n->id, stack = popStack(stack));  
}
  

void getNodeInfo(Node *n, Stack *stack)
{
  Stack *top = topStack(stack);

  top->ret.succ = n->successor;
  top->ret.pred = n->predecessor;
  top->ret.nid = n->id;

  RETURN(n->id, stack);  
}



// sending node thinks it might be n's predecessor
void notify(Node *n, Stack *stack)
{
  int id = (topStack(stack))->nodeId;


  if (n->predecessor == n->id || 
      between(id, n->predecessor, n->id, NUM_BITS)) {
    n->predecessor = id;
    if (n->successor == n->id)
      updateNodeState(n, id);
  }

  KILL_REQ(stack);
}


// periodically refresh finger table entries

void fixFingers(Node *n)
{
  if (n->status == PRESENT)
    genEvent(n->id, fixFingers_entry, (void *)NULL, 
	     Clock + unifRand(0.5*STABILIZE_PERIOD, 1.5*STABILIZE_PERIOD));
}

void fixFingers_entry(Node *n, Stack *stack)
{
  int i = unifRand(0, NUM_BITS);


  if (n->status == PRESENT)
    genEvent(n->id, fixFingers_entry, (void *)NULL, 
	     Clock + unifRand(0.5*STABILIZE_PERIOD, 1.5*STABILIZE_PERIOD));

  stack           = pushStack(stack, newStackItem(n->id, fixFingers_exit));
  stack->data.i   = i; 
  stack->data.key = fingerStart(n, i); 

  if ((i & 0x7) == 0x7) {
    CALL(getRandomActiveNodeId(), findSuccessor_entry, stack);
  } else
    CALL(n->id, findSuccessor_entry, stack);
}


void fixFingers_exit(Node *n, Stack *stack)
{
  Stack *top = topStack(stack);
  Node  *n1 = NULL;

  if (top)
    n1 = getNode(top->ret.nid);

  if (n1 && n1->status == PRESENT) 
    updateNodeState(n, n1->id);

 RETURN(n->id, stack = popStack(stack));  
}
  








