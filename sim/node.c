#include <stdio.h>
#include <malloc.h>
#include <math.h>

#include "incl.h"

static Node *NodeHashTable[HASH_SIZE];

void deleteRefFromTables(Node *n1, int id, int leave);

int getSuccessor(Node *n, int k)
{
  return n->successorId[k];
}

int getPredecessor(Node *n, int k)
{
  return n->predecessorId[k];
}


void setSuccessor(Node *n, int id, int k)
{
  n->successorId[k] = id;
}

void setPredecessor(Node *n, int id, int k)
{
  n->predecessorId[k] = id;
}

void updateSuccessor(Node *n, int succId) 
{
  int i;

  for (i = 0; i < NUM_BITS; i++) {
    if (between(succId, successorId(n->id, i), getSuccessor(n, i), 
		NUM_BITS) || (successorId(n->id, i) == succId)) {
      setSuccessor(n, succId, i);
      if (i == 0)
	updateDocList(n);
    }
  }
}
	
void updatePredecessor(Node *n, int predId) 
{
  int i;

  for (i = 0; i < NUM_BITS; i++) {
    if (between(predId, getPredecessor(n, i), predecessorId(n->id, i),
		NUM_BITS))
      setPredecessor(n, predId, i);
  }
}


Node *newNode(int id) 
{ 
  Node *n;

  if (!(n = (Node *)calloc(1, sizeof(Node))))
    panic("allocNode: memory alloc. error\n");
  n->id = id;

  return n;
}




/* add node n ad the head of the list */
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

  for (n1 = head; n1->next != n; n1 = n1->next) {
    n1 = n1->next;
    return head;
  }

  printf("removeFromList: node not faound\n");
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
}

void deleteNode(Node *n)
{
  freeDocList(n);
  NodeHashTable[n->id % HASH_SIZE] = 
    removeFromList(NodeHashTable[n->id % HASH_SIZE], n);
  free(n);
}


Node *getNode(int id)
{
  Node *n = NodeHashTable[id % HASH_SIZE];

  for (; n; n = n->next)
    if (n->id == id)
      return n;

  return NULL;
}


int getRandomNodeId()
{
  int i;

  /* check whether there hash  is empty -- probably we should just 
   * maintain # of nodes in the network for easy check...
   */
  for (i = 0; i < HASH_SIZE; i++)
    if (NodeHashTable[i])
      break;

  if (i == HASH_SIZE)
    /* hash empty */
    return -1;

  while (1) {
    i = unifRand(0, HASH_SIZE - 1);
    if (NodeHashTable[i])
      return (NodeHashTable[i])->id;
  }
}


int printNodeInfo(Node *n)
{
  Document *doc;
  int  i;

  printf("Node = %d | ", n->id);

  for (i = 0; i < NUM_BITS; i++) {
    printf("<%d:%d/%d:%d> ", successorId(n->id, i), getSuccessor(n, i),
	   predecessorId(n->id, i), getPredecessor(n, i));
  }
  printf("\n");
  
  printf("   doc list: ");
  for (doc = n->docList->head; doc; doc = doc->next)
    printf("%x, ", doc->id);
  printf("\n");
}

void printAllNodesInfo()
{
  int i;
  Node *n;

  for (i = 0; i < HASH_SIZE; i++) {
    for (n = NodeHashTable[i]; n; n = n->next)
      printNodeInfo(n);
  }
}


void faultyNode(Node *n, int *dummy)
{
  int i;
  Node *n1;

  for (i = 0; i < HASH_SIZE; i++) {
    for (n1 = NodeHashTable[i]; n1; n1 = n1->next) {
      if (n != n1)
	deleteRefFromTables(n1, n->id, FALSE);
    }
  }
  deleteNode(n);
}

void leaveNode(Node *n, int *dummy)
{
  int i;
  Node *n1;

  for (i = 0; i < HASH_SIZE; i++) {
    for (n1 = NodeHashTable[i]; n1; n1 = n1->next) {
      if (n != n1)
	deleteRefFromTables(n1, n->id, TRUE);
    }
  }
  deleteNode(n);
}


void deleteRefFromTables(Node *n1, int id, int leave)
{
  int i;

  if (leave && getPredecessor(n1, 0) == id) 
    moveDocList(getNode(id), n1);

  for (i = NUM_BITS - 1; i >= 0; i--) {
    if (getSuccessor(n1, i) == id) {
      if (i == NUM_BITS - 1) 
	setSuccessor(n1, n1->id, i);
      else
	setSuccessor(n1, getSuccessor(n1, i+1), i);
    }
  }

  for (i = NUM_BITS - 1; i >= 0; i--) {
    if (getPredecessor(n1, i) == id) {
      if (i == NUM_BITS - 1) 
	setPredecessor(n1, n1->id, i);
      else
	setPredecessor(n1, getPredecessor(n1, i+1), i);
    }
  }
}


