#include <stdio.h>
#include <stdlib.h>

#include "successor.h"
#include "util.h"

#define RECOVERY_CNT 2

void printNumDocs1(Node *nodes, int numNodes, int mod);

/* move documents from n's successor to n */
void updateDocList(Node *n)
{
  Document *doc, *d;
  Node     *s = firstSuccessor(n);
  int       flag = FALSE;

  /* move all documents x stored on s, and that are not between n and s,
   * to n, i.e., n is now closest to documents x than s
   */ 
  doc = s->docList->head;
  while (doc) {
    /* check whether doc is between n and s ... */ 
    if (!between(doc->id, n->id, s->id, NUM_BITS)) {
      /* ... if not, move document from s' document list to n's 
       * document list 
       */
      s->docList->head = doc->next;
      insertDocInList(n->docList, doc);
    } else
      break;
    doc = s->docList->head;
  }
  
  if (!(d = doc))
    return;

  while (doc = d->next) {
    if (!between(doc->id, n->id, s->id, NUM_BITS)) {
      /* move document from s' document list to n's document list */
      d->next = d->next->next;
      insertDocInList(n->docList, doc);
    } else
      d = d->next;
  }
}


/* ask node n to insert document docId */
int insertDocument(Node *n, DocId docId)
{
  Document *doc;

  /* get document location */
  n = findSuccessor(n, docId, NULL);

  /* allocate space for new document */
  if ((doc = (Document *)calloc(1, sizeof(Document))) == NULL)
    panic("insertDocument: memory allocation error for actNodes!\n");

  doc->id = docId;
  if (!insertDocInList(n->docList, doc)) {
    /* no room to insert document */
    printf("cannot insert document %x at node %x\n", doc->id, n->id); 
    free(doc);
    return FALSE;
  }
  return TRUE;
}


/* ask node n to insert document docId */
Document *findDocument(Node *n, DocId docId, int *pathLen)
{
  /* get document location */
  n = findSuccessor(n, docId, pathLen);

  return findDocInList(n->docList, docId);
}


/* ask node n to find first node after id */
Node *findSuccessor(Node *n, int id, int *pathLen)
{
  Node *s;
  int   i;

  /* if id is equal to the n's identifier, return n */
  if (n->id == id)
    return n;

  if (pathLen) (*pathLen)++;

  /* find quadrant of n to which id belongs */
  i = successorQuadrant(n, id);

  /* find first node after id  */
  for (; i; i--)
    if (s = successor(n, i)) {
      /* check whether id is between nodes n and s... */
      if (between(id, n->id, s->id, NUM_BITS)) 
	/* ... if yes, s is id's successor; return s */
	return s;
      else
	/* ... if not, id after s; recurse */
	return findSuccessor(s, id, pathLen);
    } else
      /* no node in quadrant i of s */
      if (i == 1)
	/* if this is largest quadrant, return n as next node */
	return n;
}

/* ask node n to find largest node before id; mirrors findSuccessor */
Node *findPredecessor(Node *n, int id)
{
  Node *p;
  int   i;

  if (n->id == id)
    return n;

  /* find quadrant to which id belongs */
  i = predecessorQuadrant(n, id);

  for (; i; i--)
    if (p = predecessor(n, i)) {
      /* check whether id is between nodes p and n ... */
      if (between(id, p->id, n->id, NUM_BITS))
	/* ... if yes, p is id's predecessor; return p */
	return p;
      else
	/* ... if not, id before p; recurse */
	return findPredecessor(p, id);
    } else
      if (i == 1)
	return n;
}


/* 
 * initialize n's successor and predecessor tables
 */
void collect(Node *n1, Node *n)
{
  Node *p, *p1;
  Node *s, *s1;
  int  i, id;

  /* initialize successor table */
  for (i = 1; i <= NUM_BITS; i++) {
    /* 
     * find first node after n->id + 2^{NUM_BITS - i + 1}, i.e.,
     * first node in one of the quadrants i, i-1, ..., 1 of node n
     */
    if (s1 = findSuccessor(n1, successorId(n->id, i), NULL))
      /* insert node in i-th quadrant of node n */
      successorTableAdd(n, i, s1); 
    else 
      /* if no node found try to insert n1; note that successorTableAdd
       * inserts n1 only if it belongs to quadrant i of node n
       */
      successorTableAdd(n, i, n1); 
  }

  /* initialize predecessor table; mirrors the initialization of 
   * successor table 
   */
  for (i = 1; i <= NUM_BITS; i++) {
    if (p1 = findPredecessor(n1, predecessorId(n->id, i)))
      predecessorTableAdd(n, i, p1); 
    else 
      predecessorTableAdd(n, i, n1);
  }
}


/* 
 * notify nodes of n's presence
 */
void notify(Node *n)
{
  Node *p, *p1;
  Node *s, *s1;
  int   i;

  /* notify all possible nodes for which n is their successor */
  for (i = 1; i <= NUM_BITS; i++) {

    /* get first node in quadrant -i, if any */
    if (!(p = predecessor(n, i)))
      continue;

    for (;;) {
      /* update first node known by n in its i-th quadrant */

      /* get old node known by n in its i-th quadrant, if any */
      p1 = successor(p, i);

      /* if there is no such a node or if this node is preceded by n,
       * update p's successor table */
      if (!p1 || isGreater(p1->id, n->id, NUM_BITS))
	successorTableAdd(p, i, n); 
      
      /* get first node preceding p */
      p1 = firstPredecessor(p);
      /* if (1) there is no such a node or
       *    (2) distance between p1 and p is larger than 2^{NUM_BITS} or
       *    (3) p1 doesn't belong to -i-th quadrant of n
       *    terminate
       */
      if (!p1 || isGreater(p1->id, p->id, NUM_BITS) || 
	  !inPredecessorQuadrant(p1->id, n, i))
	break;
      p = p1;
    } 
  }

  /* notify all possible nodes for which n is their predecessor
   * (mirrors the notification of all nodes for which n is their successor)
   */
  for (i = 1; i <= NUM_BITS; i++) {

    if (!(s = successor(n, i)))
      continue;

    for (;;) {
      s1 = predecessor(s, i);

      if (!s1 || isGreater(n->id, s1->id, NUM_BITS))
	predecessorTableAdd(s, i, n); 

      s1 = firstSuccessor(s);
      if (!s1 || isGreater(s->id, s1->id, NUM_BITS) || 
	  !inSuccessorQuadrant(s1->id, n, i))
	break;
      s = s1;
    } 
  }
}


/* join n via n1 */	
void join(Node *n, Node *n1)
{
#ifdef CODE_TRACE
  if (n1)
    printf("=>join(%x, %x)\n", n->id, n1->id);
  else
    printf("=>join(%x, _ )\n", n->id);    
#endif

  if (!n1) {
    /* this is the first node to join */
    n->connected = TRUE;
    return;
  }

  n->connected = TRUE;
  collect(n1, n);
  notify(n);
  updateDocList(n);
}


int main(int argc, char **argv)
{
  int     numNodes, numBatches, numDels, numDocs, nodeRatio;
  int     i, *hits, *misses, k, idx, idx2, len, l, pLen, docId;
  double *pathLen;
  Node   *nodes, *n, *n1;
  DocId  *docIds;

  if (argc != 7) {
    printf("usage: %s numNodes numDeletedNodes numDocsPerNode numBatches numNodes/numRealNodes seed\n", 
	   argv[0]);
    exit (0);
  }

  /* read number of nodes */
  if ((numNodes = atoi(argv[1])) > (1 << NUM_BITS)) {
    printf("# of nodes should be <= than 2^NUM_BITS, i.e., <= %d\n",
	    1 << NUM_BITS);
    exit(-1);
  }
  if ((numDels = atoi(argv[2])) >= numNodes) {
    printf("# of deleted nodes cannot exceed total # of nodes, i.e., %d\n",
	    numNodes);
    exit(-1);
  }
  /* read average number of documents per node */
  numDocs = atoi(argv[3]);
  if (numDocs > (1 << NUM_BITS)) {
    printf("average # of documents per node should be <= than 2^NUM_BITS, i.e., <= %d\n", 
	   (1 << NUM_BITS) / numNodes);
    exit(-1);
  }
  /* read number of batches */
  numBatches = atoi(argv[4]);
  nodeRatio = atoi(argv[5]);

  initRand(atoi(argv[6]));

  if ((nodes = (Node *)calloc(numNodes, sizeof(Node))) == NULL)
    panic("main: memory allocation error for actNodes!\n");
  if ((hits = (int *)calloc(numBatches, sizeof(int))) == NULL)
    panic("main: memory allocation error for hits!\n");
  if ((misses = (int *)calloc(numBatches, sizeof(int))) == NULL)
    panic("main: memory allocation error for misses\n");
  if ((pathLen = (double *)calloc(numBatches, sizeof(double))) == NULL)
    panic("main: memory allocation error for pathLen\n");
  if ((docIds = (DocId *)calloc(numDocs, sizeof(DocId))) == NULL)
    panic("main: memory allocation error for documents\n");
  initNet(nodes, numNodes);
  // initDocs(docIds, numDocs);

  /* build network & insert documents */
  for (i = 0; i < numNodes; i++) {
    n1 = getNode(nodes, numNodes);
    n  = getNewNode(nodes, numNodes);
    /* join n to n1 */
    join(n, n1);
/* XXXX */
  }

  for (i = 0; i < numDocs; i++) {
    
    n = getNode(nodes, numNodes);

    /* choose a document identifier */
    do {
      docId = unifRand(0, MAX_INT) & ~(~0 << NUM_BITS);
    } while (findDocument(n, docId, NULL));

    docIds[i] = docId; 
    insertDocument(n, docIds[i]);
  }

#ifdef CODE_TRACE
    printf("\n"); printAllNodesInfo(nodes, numNodes); 
#endif


  /* printf("%d %d\n", numNodes, computeMaxInDegree(nodes, numNodes)); */
  /* delete a number of numDels nodes */
  for (i = 0; i < numDels; i++) {
    n = getNode(nodes, numNodes);
    n->connected = FALSE;
  }

  /* sanityCheck(nodes, numNodes); */

  /* recover from failure */
  if (numDels && (numNodes - numDels > 1)) 
    for (k = 0; k < RECOVERY_CNT; k++) {
      for (i = 0; i < numNodes; i++) {
	if (nodes[i].connected) {
	  while ((n1 = getNode(nodes, numNodes)) == &nodes[i]);
	  collect(n1, &nodes[i]);
	  notify(&nodes[i]);
	}
      }
    }

  /* retrieve random documents from random nodes */
  for (k = 0; k < numBatches; k++) {
    len = 0;
    for (i = 0; i < BATCH_SIZE; i++) {
      
      n = getNode(nodes, numNodes);
      idx = unifRand(0, numDocs);

      pLen = 0;
      if (findDocument(n, docIds[idx], &pLen)) {
	/* printf("%d\n", pLen); */
	hits[k]++;
      } else {
	/* printf("%x ==> %x\n", nodes[idx].id, nodes[idx2].id); */
	misses[k]++;
      }
    }
    pathLen[k] = (double)len / (double)BATCH_SIZE;
  }

  printf("batch # | hits | misses | hit_rate \n");
  printf("-----------------------------------\n");

  for (k = 0; k < numBatches; k++) 
    printf("%5d %7d %7d %12f\n", k, hits[k], misses[k],
	   (double)hits[k]/(double)(hits[k] + misses[k]));

  /* sanityCheck(nodes, numNodes); */
}


