#ifndef HYPERCUBE_INCL
#define HYPERCUBE_INCL

#include "util.h"
#include "docs.h"

/* #define CODE_TRACE */
/* NUM_BITS cannot be larger than 31 */
#define NUM_BITS     24
#define BATCH_SIZE   50 /* number of retrieves in a batch */

#ifndef min
#define min(x, y)  (x < y ? x : y);
#endif


typedef struct _node {
  int           id;  /* node identifier */
  int           connected;
  DocList       docList[MAX_NUM_DOCS];
  struct _node *successor[NUM_BITS];
  struct _node *predecessor[NUM_BITS];
} Node;

/* functions implemented by hypercube_misc.c */
int successorId(int id, int i);
int predecessorId(int id, int i);
int inSuccessorQuadrant(int id, Node *n, int i);
int inPredecessorQuadrant(int id, Node *n, int i);
int successorTableAdd(Node *n, int i, Node *s);
int predecessorTableAdd(Node *n, int i, Node *p);
Node *successor(Node *n, int i);
Node *predecessor(Node *n, int i);
int successorQuadrant(Node *n, int id);
int predecessorQuadrant(Node *n, int id);
Node *firstSuccessor(Node *s);
Node *firstPredecessor(Node *p);

void initDocs(DocId *docIds, int numDocs);
void initNet(Node *nodes, int numNodes);
Node *getNewNode(Node *nodes, int numNodes);
Node *getNode(Node *nodes, int numNodes);
int printNodeInfo(Node *nodes, int idx);
void printAllNodesInfo(Node *nodes, int numNodes);
void sanityCheck(Node *nodes, int numNodes);
int   computeMaxInDegree(Node *nodes, int numNodes);

/* functions implemented in hypercube.h */
Node *findSuccessor(Node *n, int id, int *pathLen);


#endif /* HYPERCUBE_INCL */


















