#ifndef __P2PSIM_H
#define __P2PSIM_H

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

#endif // __P2PSIM_H
