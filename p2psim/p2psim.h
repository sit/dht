#ifndef __P2PSIM_H
#define __P2PSIM_H

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

class Node;

typedef unsigned IPAddress;

// mildly useful
typedef unsigned latency_t;

// time
typedef unsigned long Time;

//
// some utility functions
//

// returns the current time
Time now();

// maps ip address to Node, may return 0
Node *ip2node(IPAddress);

#ifdef WITH_DMALLOC
# include "dmalloc.h"
#endif

#endif // __P2PSIM_H
