#include "incl.h"

NeighborArgStruct *newNeighborArgStruct(int srcId, int k, int replyId) 
{ 
  NeighborArgStruct *p;

  if (!(p = (NeighborArgStruct *)calloc(1, sizeof(NeighborArgStruct))))
    panic("newNeighborArgStruct: memory alloc. error.\n");
  
  p->srcId = srcId;
  p->k = k;
  p->replyId = replyId;

  return p;
}


FindArgStruct *newFindArgStruct(int srcId, void (*fun)(), 
				int queryId, int replyId) 
{ 
  FindArgStruct *p;

  if (!(p = (FindArgStruct *)calloc(1, sizeof(FindArgStruct))))
    panic("newFindArgStruct: memory alloc. error.\n");
  
  p->srcId = srcId;
  p->fun = fun;
  p->queryId = queryId;
  p->replyId = replyId;

  return p;
}

int *newInt(int val) 
{ 
  int *p;

  if (!(p = (int *)malloc(sizeof(int))))
    panic("newInt: memory alloc. error.\n");
  
  *p = val;

  return p;
}

