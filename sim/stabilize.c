#include <stdlib.h>

#include "incl.h"

void stabilize_src1(Node *n);
void getSuccessor_dst(Node *n, NeighborArgStruct *p);
void setPredecessor_src(Node *n, NeighborArgStruct *p);
void getPredecessor_dst(Node *n, NeighborArgStruct *p);
void setSuccessor_src(Node *n, NeighborArgStruct *p);

/* 
 *  Event diagram to implement stabilization protocol 
 *
 *
 *      (NODE N)              (SUCCESSOR/PREDECESSOR OF N)
 *      
 *       ---> stabilize   -
 *       |                  -  
 *       |                    -  
 *       |                      -> getSuccessor_dst
 *       |                    -
 *       |                  -
 *       | setPredecessor_src          
 *       |
 *       |
 *       ---> stabilize 
 *       |
 *       .
 *       .
 *       .
 */

void stabilize(Node *n, void *dummy)
{
  NeighborArgStruct  *p;
  int                 i;

  for (i = 0; i < NUM_BITS; i++) {

    if (n->id != getPredecessor(n, i)) {
      p = newNeighborArgStruct(n->id, i, 0);
      genEvent(getPredecessor(n, i), getSuccessor_dst, p, 
	       Clock + intExp(AVG_PKT_DELAY));
    }
    
    if (n->id != getSuccessor(n, i)) {
      p = newNeighborArgStruct(n->id, i, 0);
      genEvent(getSuccessor(n, i), getPredecessor_dst, p, 
	       Clock + intExp(AVG_PKT_DELAY));
    }
  }

  genEvent(n->id, stabilize, NULL, 
	   Clock + unifRand(0.5*STABILIZE_PERIOD, 1.5*STABILIZE_PERIOD));
}


void getSuccessor_dst(Node *n, NeighborArgStruct *p)
{
  NeighborArgStruct *pnew = newNeighborArgStruct(n->id, p->k, 
						 getSuccessor(n, 0 /*p->k*/));

  /* XXX */
  updatePredecessor(n, p->srcId);
  updateSuccessor(n, p->srcId);

  genEvent(p->srcId, setPredecessor_src, pnew, Clock + intExp(AVG_PKT_DELAY));
}  


void setPredecessor_src(Node *n, NeighborArgStruct *p)
{
  updatePredecessor(n, p->replyId);
}

void getPredecessor_dst(Node *n, NeighborArgStruct *p)
{
  NeighborArgStruct *pnew = newNeighborArgStruct(n->id, p->k, 
						 getPredecessor(n, 0 /*p->k*/));

  /* XXX -- for fast convergence */
  updatePredecessor(n, p->srcId);
  updateSuccessor(n, p->srcId);

  genEvent(p->srcId, setSuccessor_src, pnew, Clock + intExp(AVG_PKT_DELAY));
}  


void setSuccessor_src(Node *n, NeighborArgStruct *p)
{
  updateSuccessor(n, p->replyId);
}

