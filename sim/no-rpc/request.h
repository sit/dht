#ifndef  INCL_REQUEST
#define INCL_REQUEST
 

// Request data structure
//  
// - a request r is resolved when it arrives at
//   node n, where n < r.x <= n.successor
// - after a request is resolved, it is sent back
//   to its initiator   

typedef struct request_ {
  ID  x;         // request value 
#define REQ_TYPE_STABILIZE   0
#define REQ_TYPE_INSERTDOC   1
#define REQ_TYPE_FINDDOC     2
#define REQ_TYPE_REPLACESUCC 3
#define REQ_TYPE_REPLACEPRED 4
#define REQ_TYPE_SETSUCC     5
  int type;      // request type
#define REQ_STYLE_ITERATIVE 0
#define REQ_STYLE_RECURSIVE 1
  int style;     // whether the request is reslved by using
                 // an iterative or recursive algorithm 
  ID  initiator; // node initiating the request
  ID  sender;    // sender of current request
  ID  pred;      // best known predecessor of x
  ID  succ;      // best known successor of x
  int done;      // TRUE after message has arrived at node n
                 // where n < r.x <= n.successor   
  // the following fields are used for retries
  ID  dst;
  ID  del;
  struct {
#define STACKNODE_SIZE 20
    ID nodes[STACKNODE_SIZE];
    int num;  // number of nodes in the stack
  } stack;

  struct request_ *next; 
} Request;
  
typedef struct requestList_ {
  Request *head;
  Request *tail;
} RequestList;

#endif // INCL_REQUEST







