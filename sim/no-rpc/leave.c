#include <stdlib.h>
#include "incl.h"

void leave(Node *n, void *dummy) {
  Request *r;

  moveDocList(n, getNode(getSuccessor(n)));

#ifdef OPTIMIZATION
  // announce predecessor about the new successor
  r = newRequest(getSuccessor(n), REQ_TYPE_REPLACESUCC, 
		 REQ_STYLE_RECURSIVE, n->id);
  genEvent(getPredecessor(n), insertRequest, (void *)r, 
	   Clock + intExp(AVG_PKT_DELAY));
  // announce successor about the new predecessor
  r = newRequest(getPredecessor(n), REQ_TYPE_REPLACEPRED, 
		 REQ_STYLE_RECURSIVE, n->id);
  genEvent(getSuccessor(n), insertRequest, (void *)r, 
	   Clock + intExp(AVG_PKT_DELAY));
#endif // OPTIMIZATION

  n->status = ABSENT;
}

void faultyNode(Node *n, void *dummy) {
  n->status = ABSENT;
}







