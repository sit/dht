#ifndef INCL_STACK
#define INCL_STACK


/* upon a new function call, the current state and returning 
 * parameters are saved 
 */  
typedef struct stack_ {
  void (*fun)(); /* function to be called upon return */
  int  nodeId;   /* id of node where fun is invoked */
  /* arguments */
  struct {
    int succ;  /* successor of nodeId */
    int pred;  /* predecessor of nodeId */
    int i;     /* used to implement loops */
    int nid;
    int key; 
  } data;
  /* return parameters */
  struct {
    int nid;   /* node identifier */
    int found; /* whether a node/key has been found */
    int succ;  /* successor of nId */
    int pred;  /* predecessor of nId */
  } ret;
  struct stack_ *next;
} Stack;


#define RETURN(nodeId, stack) { \
  returnStack(nodeId, stack); \
  return; \
}


#define KILL_REQ(stack) { \
  if (stack) freeStack(stack); \
  return; \
}



#define CALL(nodeId, fun, stack) { callStack(nodeId, fun, stack); } 


#endif /* INCL_STACK */


