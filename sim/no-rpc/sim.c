/*
 *
 * Copyright (C) 2001 Ion Stoica (istoica@cs.berkeley.edu)
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

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
      if ((n = getNode(ev->nodeId)))
	ev->fun(n, ev->params);
      free(ev);
    }
  }
  return 0;
}


void exitSim(void)
{
  printf("Exit: %f\n", Clock);
  printAllNodesInfo();
  printPendingDocs();
  exit(0);
}














