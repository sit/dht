#ifndef INCL_EVENT_TYPES_
#define INCL_EVENT_TYPES_


typedef struct fcb_ {
  int  nodeId;
  void (*fun)();
  struct fcb_ *prevfcb;
} FCB;

typedef struct {
  int  queryId;  /* request id */
  int  replyId;  /* result (successor/predecessor of queryId) */
  FCB *fcb;      /* fcb of function who invoked this event */ 
} FindArgStruct;

typedef struct {
  int origId;
  int srcId;
  void (*fun)();       
  int replyId; 
} NeighborArgStruct;

typedef struct {
  int id;
  int fingerIdx;
} NotifyArgStruct;

#endif /* INCL_EVENT_TYPES_ */ 
