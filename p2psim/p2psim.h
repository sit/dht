#ifndef __P2PSIM_H
#define __P2PSIM_H

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include "tmgdmalloc.h"

// multiply per-thread stack size by ...
#define DEFAULT_THREAD_STACKSIZE 4096
#define THREAD_MULTIPLY 3

extern unsigned verbose;
#define DEBUG(x) if(verbose >= (x)) cout

class Node;

typedef unsigned IPAddress;

// time, in milliseconds
typedef unsigned long long Time;

//
// some utility functions
//

// returns the current time
Time now();

// tries to clean things up cleanly
void graceful_exit(void*);

#ifdef WITH_DMALLOC
# include "dmalloc.h"
#endif

#endif // __P2PSIM_H
