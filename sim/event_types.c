#include "incl.h"

NeighborArgStruct *newNeighborArgStruct(int origId, int srcId, 
				void (*fun)(), int replyId) 
{ 
  NeighborArgStruct *p;

  if (!(p = (NeighborArgStruct *)calloc(1, sizeof(NeighborArgStruct))))
    panic("newNeighborArgStruct: memory alloc. error.\n");
  
  p->origId = origId;
  p->srcId = srcId;
  p->fun = fun;
  p->replyId = replyId;

  return p;
}

FindArgStruct *newFindArgStruct(int origId, int srcId, void (*fun)(), 
				int queryId, int replyId) 
{ 
  FindArgStruct *p;

  if (!(p = (FindArgStruct *)calloc(1, sizeof(FindArgStruct))))
    panic("newFindArgStruct: memory alloc. error.\n");
  
  p->origId = origId; 
  p->srcId = srcId;
  p->fun = fun;
  p->queryId = queryId;
  p->replyId = replyId;

  return p;
}


NotifyArgStruct *newNotifyArgStruct(int fingerIdx, int id)
{ 
  NotifyArgStruct *p;

  if (!(p = (NotifyArgStruct *)calloc(1, sizeof(NotifyArgStruct))))
    panic("newNotifyArgStruct: memory alloc. error.\n");
  
  p->id = id; 
  p->fingerIdx = fingerIdx;

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


