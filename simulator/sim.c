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
#include <stdlib.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include <string.h>
#include <signal.h>

#define DECL_VAR
#include "incl.h"

extern int num_successors;

void sigint_handler(int);

void usage(char* cmd)
{
    fprintf(stderr,
	    "usage: %s "
	    "-i inputfile "
	    "[-s random seed] "
	    "[-n num_successors] "
	    "[-l max_location_cache] "
	    "[-g num_fingers] "
	    "[-f] "
	    "\n", cmd);

    exit(-1);
}


int main(int argc, char **argv) 
{
  Event *ev;
  Node  *n;
  extern char* optarg;
  char* inputfile = "";
  int seed = 0;
  char ch;
  
  while ((ch = getopt(argc, argv, "i:s:l:g:n:f")) != EOF) {

    switch(ch) {

	case 'i':
	    inputfile = optarg;
	    break;

	case 's':
	    seed = atoi(optarg);
	    break;

	case 'l':
	    max_location_cache = atoi(optarg);
	    break;
   
	case 'g':
	    num_fingers = atoi(optarg);
	    break;

	case 'n':
	    num_successors = atoi(optarg);
	    break;

	case 'f':
	    // MW. 4/10/03. when we have millions of documents, a llist is not
	    // the best way to store documents. in these tests, we care only 
	    // about routing performance, so every lookup is assumed to
	    // succeed. this flag tells sim to not store any docs in
	    // its doc lists.
	    fake_doc_list_ops = 1;
	    break;

	default:
	    usage(argv[0]);
    }

  }

  // the only mandatory argument
  if (!strcmp(inputfile, ""))
      usage(argv[0]);

  EventQueue.heaps = initEventQueue();
  EventQueue.size = MAX_NUM_ENTRIES;

  signal(SIGINT, sigint_handler);
  initRand(seed);
  readInputFile(inputfile);

  // this is a rough check and ignores the fact that successors will
  // overlap with fingers. but if the user satisfies this check,
  // correctness is ensured. it's a conservative estimate, in other words
  if (max_location_cache < (num_successors - 1) + num_fingers) {
      printf("there are %d fingers and %d successors.\nThe location cache "
	     "has to be at least as big as the number of fingers plus\n"
	     "the number of successors minus one\n", 
	     num_fingers, num_successors);
      exit(-1);
   }

  printf("simulation beginning.\n"
	 "num_succs=%d, seed=%d location_cache=%d num_fingers=%d\n", 
	 num_successors, seed, max_location_cache, num_fingers);

  while (Clock < MAX_TIME) {
    double clock_evtime;
    ev = getEvent(&EventQueue, Clock, &clock_evtime);
    if (!ev) 
      Clock = (double)((int)(Clock/ENTRY_TUNIT) + 1.0)*ENTRY_TUNIT; 
    else {
      Clock = clock_evtime;
      {
        static double tsec = 0.;
        if (Clock/1000000. > tsec) {
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

// put this in as a placeholder 
void sigint_handler(int param)
{
    fprintf(stderr, "Exiting . . .\n");
    exit(1);
}


