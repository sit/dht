#ifndef INCL_EVENT_TYPES_
#define INCL_EVENT_TYPES_

typedef struct {
  int srcId;
  int k;       
  int replyId; /* return k-th successor/predecessor of current node to srcId */
} NeighborArgStruct;



typedef struct {
  int   srcId;    /* node who triggered event */
  void  (*fun)(); /* function which will be invoked when reply is received
		     by requester */
  int   queryId;
  int   replyId;   /* result (successor/predecessor of queryId */
} FindArgStruct;

#endif /* INCL_EVENT_TYPES_ */ 
