#include <stdlib.h>
#include "incl.h"

void leave_exit(Node *n, Stack *stack);
void leaveNode(Node *n, int *dummy);
void deleteRefFromTables(Node *n1, int id);
void leave_notify_succ(Node *n, Stack *stack);
void leave_wait(Node *n, Stack *stack);

Node *NodeHashTable[HASH_SIZE];

void leave(Node *n, int *dummy) {
  n->status = TO_LEAVE;
  genEvent(n->id, leave_wait, (void *)NULL, Clock + LEAVE_WAIT);

  printf("leave = %d, %f\n", n->id, Clock);
}

void leave_wait(Node *n, Stack *stack)
{
  stack = pushStack(stack, newStackItem(n->id, leave_exit));
  stack->data.nid = n->id;
  stack->data.succ = n->successor;
  stack->data.pred = n->predecessor;

  CALL(n->successor, leave_notify_succ, stack);
}


void leave_notify_succ(Node *n, Stack *stack)
{
  Stack *top = topStack(stack);

 if (n->status == PRESENT) {
    if (top->nodeId == n->predecessor) {
      deleteRefFromTables(n, top->nodeId);
      updateNodeState(n, top->data.pred);
    } 
    top->fun = leave_exit;
    RETURN(top->nodeId, stack);
  } else {
    // delay the leave 
    genEvent(top->nodeId, leave, (void *)NULL, 
	     Clock + unifRand(0.5*STABILIZE_PERIOD, 1.5*STABILIZE_PERIOD));
    KILL_REQ(stack);
  }
}

void leave_exit(Node *n, Stack *stack)
{
  n->status = ABSENT;
  freeStack(stack);
  leaveNode(n, NULL); 
}
 
void leaveNode(Node *n, int *dummy)
{
  int i;
  Node *n1;

  if (n->finger[0] != n->id)
    moveDocList(n, getNode(n->finger[0]));

  /* update neigbour tables */
  for (i = 0; i < HASH_SIZE; i++) {
    for (n1 = NodeHashTable[i]; n1; n1 = n1->next) {
      if (n != n1) {
	deleteRefFromTables(n1, n->id);
      }
    }
  }
  deleteNode(n);
}


void deleteRefFromTables(Node *n1, int id)
{
  int i;

  for (i = NUM_BITS - 1; i >= 0; i--) {
    if (n1->finger[i] == id) {
      if (i == NUM_BITS - 1)
	n1->finger[i] = n1->id;
      else {
	if (i == 0) {
	  n1->successor = n1->finger[0] = n1->finger[1]; 
	} else
	  n1->finger[i] = n1->finger[i+1];
      }
    }
  }

  if (n1->predecessor == id) {
    if (getNode(id))
      n1->predecessor = getNode(id)->predecessor;
  } 

  if ((n1->successor == n1->id) && (n1->predecessor != n1->id)) {
    for (i = 0; i < NUM_BITS; i++) 
      n1->finger[i] = n1->predecessor;
    n1->successor = n1->finger[0];
  }
 
  updateDocList(n1);
}














