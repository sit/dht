#include <stdio.h>
#include <malloc.h>
#include <math.h>

#include "incl.h"

Node *NodeHashTable[HASH_SIZE];

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
  int i, cnt = 0, idx;

  idx = unifRand(0, HASH_SIZE);

  for (i = idx; i < HASH_SIZE; i++)
    if (NodeHashTable[i] && NodeHashTable[i]->status == PRESENT)
      return (NodeHashTable[i])->id;
  
  for (i = idx; i; i--)
    if (NodeHashTable[i] && NodeHashTable[i]->status == PRESENT)
      return (NodeHashTable[i])->id;

  return -1;
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
  return -1;
}


/*******************************************************/
/*      update node state
/*******************************************************/

void updateNodeState(Node *n, int id)
{
  // ??? code to be added
  updateDocList(n);
}


int printNodeInfo(Node *n)
{
  Document *doc;
  int  i;

  if (n->status == ABSENT)
    return;

  if (getSuccessor(n) == n->id)
    printf("o");

  printf("Node = %d\n", n->id);

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


