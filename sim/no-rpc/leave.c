#include <stdlib.h>
#include "incl.h"

void leave(Node *n, void *dummy) {
  moveDocList(n, getNode(getSuccessor(n)));
  n->status = ABSENT;
}

void faultyNode(Node *n, void *dummy) {
  n->status = ABSENT;
}







