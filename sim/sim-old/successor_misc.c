#include <stdlib.h>

#include "successor.h"

/* compute (id + i) mod 2^NUM_BITS */
int successorId(int id, int i)
{
  id = id + (1 << (NUM_BITS - i));

  if (id >= (1 << NUM_BITS))
      id -= (1 << NUM_BITS);

  return id;  
}

/* compute (id - i) mod 2^NUM_BITS */
int predecessorId(int id, int i)
{
  id = id - (1 << (NUM_BITS - i));

  if (id < 0)
      id += (1 << NUM_BITS);

  return id;
}

/* check whether id belongs to i-th successor quadrant of node n.
 * i-th successor quadrant of node n represents the interval 
 * [(n->id + 2^{NUM_BITS-i+1}) mod 2^{NUM_BITS}, 
 *  (n->id + 2^{NUM_BITS-i+2}) mod 2^{NUM_BITS})
 */
int inSuccessorQuadrant(int id, Node *n, int i)
{
  if (i == 1) { 
    if (isGreaterOrEqual(id, successorId(n->id, i), NUM_BITS))
      return TRUE;
  } else
    if (isGreaterOrEqual(id, successorId(n->id, i), NUM_BITS) &&
	isGreater(successorId(n->id, i-1), id, NUM_BITS))
      return TRUE;
  return FALSE;
}

/* check whether id belongs to i-th predecessor quadrant of node n.
 * i-th predecessor quadrant of node n represents the interval 
 * [(n->id - 2^{NUM_BITS-i+1}) mod 2^{NUM_BITS}, 
 *  (n->id - 2^{NUM_BITS-i+2}) mod 2^{NUM_BITS})
 */
int inPredecessorQuadrant(int id, Node *n, int i)
{
  if (i == 1) {
    if (isGreaterOrEqual(predecessorId(n->id, i), id, NUM_BITS))
      return TRUE;
  } else
    if (isGreaterOrEqual(predecessorId(n->id, i), id, NUM_BITS) &&
	isGreater(id, predecessorId(n->id, i - 1), NUM_BITS))
      return TRUE;
  return FALSE;
}

int successorTableAdd(Node *n, int i, Node *s)
{
  if (i == 1) { 
    if (isGreaterOrEqual(s->id, successorId(n->id, i), NUM_BITS))
      n->successor[i - 1] = s;
  } else
    if (isGreaterOrEqual(s->id, successorId(n->id, i), NUM_BITS) &&
	isGreater(successorId(n->id, i-1), s->id, NUM_BITS))
      n->successor[i - 1] = s;
}

int predecessorTableAdd(Node *n, int i, Node *p)
{
  if (i == 1) {
    if (isGreaterOrEqual(predecessorId(n->id, i), p->id, NUM_BITS))
      n->predecessor[i - 1] = p;
  } else
    if (isGreaterOrEqual(predecessorId(n->id, i), p->id, NUM_BITS) &&
	isGreater(p->id, predecessorId(n->id, i - 1), NUM_BITS))
      n->predecessor[i - 1] = p;
}

/* get smallest node greater than (n->id + 2^{NUM_BITS-i+1}) mod 2^NUM_BITS */
Node *successor(Node *n, int i)
{
  Node *n1, *s = n->successor[i - 1];

  if (s)
    if (!s->connected) 
      n->successor[i - 1] = NULL;      
    else {
      /* predecessor of s in quadrant i might be n */
      if (!s->predecessor[i-1] || 
	  isGreater(n->id, s->predecessor[i-1]->id, NUM_BITS))
	s->predecessor[i-1] = n;
    }

  return n->successor[i - 1];
}

/* get largest node smaller than (n->id - 2^{NUM_BITS-i+1}) mod 2^NUM_BITS */
Node *predecessor(Node *n, int i)
{
  Node *n1, *p = n->predecessor[i - 1];

  if (p)
    if (!p->connected) 
      n->predecessor[i - 1] = NULL;
    else {
      /* predecessor of s in quadrant i is n */
      if (!p->successor[i-1] || 
	  isGreater(p->successor[i-1]->id, n->id, NUM_BITS))
	p->successor[i-1] = n;
    }

  return n->predecessor[i - 1];
}


/* return successor quadrant of node n containing id */
int successorQuadrant(Node *n, int id)
{
  int i;
  
  for (i = 1; i <= NUM_BITS; i++) {
    if (isGreaterOrEqual(id, successorId(n->id, i), NUM_BITS))
      break;
  }
  return i;
}

/* return predecessor quadrant of node n containing id */
int predecessorQuadrant(Node *n, int id)
{
  int i;

  for (i = 1; i <= NUM_BITS; i++) {
    if (isGreaterOrEqual(predecessorId(n->id, i), id, NUM_BITS))
      break;
  }
  return i;
}


/* find first successor of node s */
Node *firstSuccessor(Node *s)
{
  int i;

  for (i = NUM_BITS - 1; i > -1; i--)
    if (s->successor[i])
      return s->successor[i];

  return NULL;
}

/* find first predecessor of node p */
Node *firstPredecessor(Node *p)
{
  int i;

  for (i = NUM_BITS - 1; i > -1; i--)
    if (p->predecessor[i])
      return p->predecessor[i];

  return NULL;
}


/*========================================================*/

void initDocs(DocId *docIds, int numDocs)
{
  int i, k, flag;

  for (i = 0; i < numDocs; i++) {
    do {
      flag = FALSE;

      docIds[i]  = unifRand(0, MAX_INT) & ~(~0 << NUM_BITS);
      for (k = 0; k < i; k++) 
	if (docIds[i] == docIds[k])
	  flag = TRUE;
    } while (flag);
  }
}

/*
 * initialize network topology and node content
 */
void initNet(Node *nodes, int numNodes)
{
  int i, k, flag;

  for (i = 0; i < numNodes; i++) {
    do {
      flag = FALSE;
      nodes[i].id = unifRand(0, MAX_INT) & ~(~0 << NUM_BITS);
      
      for (k = 0; k < i; k++) 
	if (nodes[i].id == nodes[k].id)
	  flag = TRUE;
    } while (flag);
  }
}

/* get a new node to be inserted in the network */
Node *getNewNode(Node *nodes, int numNodes)
{
  int idx = unifRand(0, numNodes); /* search for an unconnected 
				    *  node close to idx 
				    */
  int i;

  for (i = idx; i < numNodes && nodes[i].connected; i++);
  if (i < numNodes) 
    return &nodes[i];
  for (i = idx; i > -1 && nodes[i].connected; i--);
  if (i > -1) 
    return &nodes[i];

  return NULL;
}

/* get a node already connected to the network */    
Node *getNode(Node *nodes, int numNodes)
{
  int idx = unifRand(0, numNodes), i; 

  for (i = idx; i < numNodes && !nodes[i].connected; i++);
  if (i < numNodes)
    return &nodes[i];
  for (i = idx; i > -1 && !nodes[i].connected; i--);
  if (i > -1) 
    return &nodes[i];

  return NULL;
}



int printNodeInfo(Node *nodes, int idx)
{
  Node *n = &nodes[idx], *s, *p;
  Document *doc;
  int  i;

  if (!n->connected) 
    return;

  printf("Node = %x | ", n->id);

  for (i = 1; i <= NUM_BITS; i++) {
    s = successor(n, i);
    p = predecessor(n, i);
    if (s && p)
      printf("<%x:%x/%x:%x> ", successorId(n->id, i), s->id,
	     predecessorId(n->id, i), p->id);
    else if (s)
      printf("<%x:%x/%x:-> ", successorId(n->id, i), s->id,
	     predecessorId(n->id, i));
    else if (p)
      printf("<%x:-/%x:%x> ", successorId(n->id, i), 
	     predecessorId(n->id, i), p->id);
    else
      printf("<%x:-/%x:-> ", successorId(n->id, i),
	     predecessorId(n->id, i));
  }
  printf("\n");
  
  printf("   doc list: ");
  for (doc = n->docList->head; doc; doc = doc->next)
    printf("%x, ", doc->id);
  printf("\n");

}

void printAllNodesInfo(Node *nodes, int numNodes)
{
  int k;

  for (k = 0; k < numNodes; k++) 
    printNodeInfo(nodes, k);
}

void printNumDocs(Node *nodes, int numNodes)
{
  int len, k;
  Document *doc;
  Node     *n;

  for (k = 0; k < numNodes; k++) { 
    n = &nodes[k];
    if (n->connected) {
      for (doc = n->docList->head, len = 0; doc; doc = doc->next, len++);
      printf("%d\n", len); 
    }
  }
}

void printNumDocs1(Node *nodes, int numNodes, int mod)
{
  int len, k, i, sum;
  Document *doc;
  Node     *n;

  i = 0;
  sum = 0;
  for (k = 0; k < numNodes; k++) { 
    n = &nodes[k];
    if (i < mod) {
      i++;
      if (n->connected) { 
	for (doc = n->docList->head, len = 0; doc; doc = doc->next, len++);
	sum += len;
      }
    } 
    if (i == mod) {
      printf("%d\n", sum); 
      i = sum = 0;
    }
  }
}


/* 
 * check whether any nodes are disconnected, i.e., they are not in the 
 * routing table of any other nodes 
 */
void sanityCheck(Node *nodes, int numNodes)
{
  int i, j, l, found;

  printf("Disconnected nodes (i.e., not referred by any other node in net):\n");
  printf("------------------------------------------------------------------\n");

  for (i = 0; i < numNodes; i++) {
    if (nodes[i].connected) {
      found = FALSE;
      for (j = 0; j < numNodes; j++) {
	if (j != i && nodes[j].connected) {
	  for (l = 0; l < NUM_BITS; l++) {
	    if (nodes[j].predecessor[l] && 
		(nodes[i].id == nodes[j].predecessor[l]->id)) {
	      found = TRUE;
	      break;
	    }
	    if (nodes[j].successor[l] && 
		(nodes[i].id == nodes[j].successor[l]->id)) {
	      found = TRUE;
	      break;
	    }
	  }
	  if (found) break;
	}
      }
      if (!found)
	printf("%x\n", nodes[i].id);
    }
  }
}


/* compute maximum in-degree of a node; i.e., maximum numbers of times
 * a node is referred by other nodes in their routing tables
 */
int computeMaxInDegree(Node *nodes, int numNodes)
{
  int *v, i, k, l, max;

  if ((v = (int *)calloc(numNodes, sizeof(int))) == NULL)
    panic("computeMaxInDegree: memory allocation error\n");

  for (i = 0; i < numNodes; i++) {
    for (k = 0; k < NUM_BITS; k++) {
      if (nodes[i].successor[k])
	for (l = 0; l < numNodes; l++)
	  if (nodes[i].successor[k] == &nodes[l]) {
	    v[l]++;
	    break;
	  }
    }
    for (k = 0; k < NUM_BITS; k++) {
      if (nodes[i].predecessor[k])
	for (l = 0; l < numNodes; l++)
	  if (nodes[i].predecessor[k] == &nodes[l]) {
	    v[l]++;
	    break;
	  }
    }
  }

  max = 0;
  for (i = 0; i < numNodes; i++)
    if (v[i] > max)
      max = v[i];

  return max;
}

