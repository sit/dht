
#include <stdlib.h>

#include "incl.h"

void join1(Node *n, FindArgStruct *p);
void join2(Node *n, FindArgStruct *p);
void updateSuccessor1(Node *n, FindArgStruct *p);
void updatePredecessor1(Node *n, FindArgStruct *p);

void join_ev(Node *n, int *nodeId)
{
  join(n, *nodeId);
}

/* node n joins through other node nodeId */
void join(Node *n, int nodeId) {
  int i;

  /* initialize all entries */
  for (i = 0; i < NUM_BITS; i++) {
    setSuccessor(n, n->id, i);
    setPredecessor(n, n->id, i);
  }
  if (nodeId != -1)
    findPredecessor(n, nodeId, n->id, join1);

  /* invoke stabilize */
  genEvent(n->id, stabilize, (void *)NULL, 
	   Clock + unifRand(0.5*STABILIZE_PERIOD, 1.5*STABILIZE_PERIOD));

}

void join1(Node *n, FindArgStruct *p) 
{
  updatePredecessor(n, p->replyId);
  findSuccessor(n, p->srcId, n->id, join2);
}

void join2(Node *n, FindArgStruct *p)
{
  int predId = getPredecessor(n, 0);

  /* 
   * check wether n is between its predecessor (predId) and the 
   * successor of predId (i.e., p->replyId) 
   * if yes, p->replyId is n's successor
   * otherwise, p->replyId is a better predecessor for n; update n's
   * predecessor and iterate 
   */
  if (between(n->id, predId, p->replyId, NUM_BITS) || (predId == p->replyId)) {

    updateSuccessor(n, p->replyId);
    boostrap(n, p->replyId);
    boostrap(n, getPredecessor(n, 0));
    notify(getNode(p->replyId), n->id);
    notify(getNode(predId), n->id);

  } else {
    /* p->replyId is still a predecessor of n->id; iterate */
    updatePredecessor(n, predId);
    findSuccessor(n, predId, n->id, join2);
  }
}


void boostrap(Node *n, int nodeId)
{
  int i;

  for (i = 1; i < NUM_BITS; i++) {
    findSuccessor(n, nodeId, successorId(n->id, i), updateSuccessor1);
    findPredecessor(n, nodeId, predecessorId(n->id, i), updatePredecessor1);
  }
}

void updateSuccessor1(Node *n, FindArgStruct *p) 
{
  if (n->id != p->replyId) 
    updateSuccessor(n, p->replyId);
}

void updatePredecessor1(Node *n, FindArgStruct *p) 
{
  if (n->id != p->replyId) 
    updatePredecessor(n, p->replyId);
}
	

/* XXX notify directly -- to be changed later */
void notify(Node *n, int nodeId)
{
  updateSuccessor(n, nodeId);
  updatePredecessor(n, nodeId);
}
