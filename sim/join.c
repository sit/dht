#include "incl.h"

void join(Node *n, int *nodeId) {
  int i;
  Node *n1;

  if ((!(n1 = getNode(*nodeId))) || (n1 && n->status == ABSENT))
    // probably nodeId has been deleted in the meantime
    n1 = getNode(getRandomActiveNodeId());

  if (n1)
    insertFinger(n, n1->id);
  n->status = PRESENT;
  stabilize(n);
  processRequest(n);
}

