#include "incl.h"

void findSuccessor_dst(Node *n, FindArgStruct *p);
void findSuccessor_src(Node *n, FindArgStruct *p);
void findPredecessor_dst(Node *n, FindArgStruct *p);
void findPredecessor_src(Node *n, FindArgStruct *p);
void findSuccessor_end(Node *n, FindArgStruct *p);
void findPredecessor_end(Node *n, FindArgStruct *p);

/* 
 *  Event diagram to implement find successor protocol 
 *   (find predecessor is identical)
 *
 *      (NODE N)                   (OTHER NODE M)
 *      
 *  findSuccessor     -
 *                      -   
 *                        -  
 *                          -> findSuccessor_dst
 *                         -
 *                       -
 *  findSuccessor_src <- 
 *                       -
 *                        -  
 *                          -> findSuccessor_dst
 *                         -
 *                        .
 *                       .
 * findSuccessor_end  <- 
 *                                       
 *                                            
 */


/* get clossest id in idTable that preceeds x, if id is better than s */
int getClosestSuccessor(int *idTable, int x, int s)
{
  int i;

  for (i = 0; i < NUM_BITS; i++)
    /* is idTable[i] a closest successor for x than s */
    if (between(idTable[i], x, s, NUM_BITS))
      s = idTable[i];

  return s;
}


/* get clossest id in idTable that preceeds x, if id is better than p */
int getClosestPredecessor(int *idTable, int x, int p)
{
  int i;

  for (i = 0; i < NUM_BITS; i++)
    /* is idTable[i] a closest predecessor for x than p */
    if (between(idTable[i], p, x, NUM_BITS))
      p = idTable[i];

  return p;
}

int lookupClosestSuccessor(Node *n, int x)
{
  return getClosestSuccessor(n->successorId, x, n->id);
}

int lookupClosestPredecessor(Node *n, int x)
{
  return  getClosestPredecessor(n->predecessorId, x, n->id);
}


/* 
 * ask nodeId for successor of x
 * upon result return call fun()
 */
void findSuccessor(Node *n, int nodeId, int x, void (*fun)())
{
  FindArgStruct *p = newFindArgStruct(n->id, fun, x, n->id);

  /* generate event */
  genEvent(nodeId, findSuccessor_dst, p, Clock + intExp(AVG_PKT_DELAY));
}


/* ask node id for successor of x */
void findSuccessor_dst(Node *n, FindArgStruct *p)
{
  FindArgStruct *pnew;
  int            succ = lookupClosestSuccessor(n, p->queryId);
  
  pnew = newFindArgStruct(n->id, p->fun, p->queryId, succ);

  if (succ == n->id || pnew->replyId == p->srcId)
    genEvent(p->srcId, findSuccessor_end, pnew, 
	     Clock + intExp(AVG_PKT_DELAY));
  else
    genEvent(p->srcId, findSuccessor_src, pnew, 
	     Clock + intExp(AVG_PKT_DELAY));
}

/* ask node id for successor of x */
void findSuccessor_src(Node *n, FindArgStruct *p)
{
  FindArgStruct *pnew;

  pnew = newFindArgStruct(n->id, p->fun, p->queryId, p->replyId);

  /* generate event */
  genEvent(p->replyId, findSuccessor_dst, pnew, 
	   Clock + intExp(AVG_PKT_DELAY));
}


/* ask node id for successor of x */
void findSuccessor_end(Node *n, FindArgStruct *p)
{
  p->fun(n, p);
}

/******* predecessor ****/

/* ask nodeId for predecessor of x */
void findPredecessor(Node *n, int nodeId, int x, void (*fun)())
{
  FindArgStruct *p = newFindArgStruct(n->id, fun, x, n->id);

  /* generate event */
  genEvent(nodeId, findPredecessor_dst, p, Clock + intExp(AVG_PKT_DELAY));
}


/* ask node id for predecessor of x */
void findPredecessor_dst(Node *n, FindArgStruct *p)
{
  FindArgStruct *pnew;
  int            pred = lookupClosestPredecessor(n, p->queryId);
  
  pnew = newFindArgStruct(n->id, p->fun, p->queryId, pred);

  if (pred == n->id || pnew->replyId == p->srcId)
    genEvent(p->srcId, findPredecessor_end, pnew, 
	     Clock + intExp(AVG_PKT_DELAY));
  else
    genEvent(p->srcId, findPredecessor_src, pnew, 
	     Clock + intExp(AVG_PKT_DELAY));
}

/* ask node id for predecessor of x */
void findPredecessor_src(Node *n, FindArgStruct *p)
{
  FindArgStruct *pnew;

  pnew = newFindArgStruct(n->id, p->fun, p->queryId, p->replyId);

  /* generate event */
  genEvent(p->replyId, findPredecessor_dst, pnew, 
	   Clock + intExp(AVG_PKT_DELAY));
}


/* ask node id for predecessor of x */
void findPredecessor_end(Node *n, FindArgStruct *p)
{
  p->fun(n, p);
}


