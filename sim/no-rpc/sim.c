#include <stdio.h>
#include <ctype.h>
#include <malloc.h>
#include <stdlib.h>

#define DECL_VAR
#include "incl.h"

int main(int argc, char **argv) 
{
  Event *ev;
  Node  *n;
  int   i = 0;


  if (argc != 3) {
    printf("usage: %s input_file seed\n", argv[0]);
    exit (-1);
  }

  EventQueue.q = initEventQueue();
  EventQueue.size = MAX_NUM_ENTRIES;

  initRand(atoi(argv[2]));
  

  readInputFile(argv[1]);


  while (Clock < MAX_TIME) {
    ev = getEvent(&EventQueue, Clock);
    if (!ev) 
      Clock = (double)((int)(Clock/ENTRY_TUNIT) + 1.0)*ENTRY_TUNIT; 
    else {
      Clock = ev->time;
      {
        static double tsec = 0.;
        if (ev->time/1000000. > tsec) {
          tsec += 1.0;
	}
      }
      if (ev->fun == exitSim) {
	exitSim();
      }
      if (n = getNode(ev->nodeId)) 
	ev->fun(n, ev->params);
      free(ev);
    }
  }
}


void exitSim(void)
{
  printf("Exit: %f\n", Clock);
  printAllNodesInfo();
  printPendingDocs();
  exit(0);
}














