#include <stdio.h>
#include <ctype.h>
#include <malloc.h>
#include <stdlib.h>

#define DECL_VAR
#include "incl.h"

int main(int argc, char **argv) 
{
  Node *n = newNode(5);
  ID   pred, succ;
  int  i;
#define NUM_VALS 5
  int vals[NUM_VALS] = {1, 6, 11, 5, 15};

  n->status = PRESENT;
  printNodeInfo(n);

  Clock = 10;
  insertFinger(n, 8);
  Clock = 12;
  insertFinger(n, 6);
  Clock = 13;
  insertFinger(n, 7);
  Clock = 14;
  insertFinger(n, 9);
  Clock = 9;
  insertFinger(n, 14);
  printNodeInfo(n);

  Clock = 17;
  insertFinger(n, 10);
  printNodeInfo(n);

  Clock = 18;
  insertFinger(n, 13);
  printNodeInfo(n);

  Clock = 19;
  insertFinger(n, 15);
  printNodeInfo(n);

  for (i = 0; i < NUM_VALS; i++) {
    getNeighbors(n, vals[i], &pred, &succ);
    printf("pred(%d) = %d, succ(%d) = %d\n", 
	   vals[i], pred, vals[i], succ);
  }  

  insertRequest(n, newRequest(1, 0, 0, n->id));
  insertRequest(n, newRequest(2, 0, 0, n->id));
  insertRequest(n, newRequest(3, 0, 0, n->id));

  printNodeInfo(n);

  free(getRequest(n));
  free(getRequest(n));

  printNodeInfo(n);

}








