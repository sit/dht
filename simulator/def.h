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

#ifndef INCL_DEF
#define INCL_DEF

#define EOL          0xa

#ifndef MAX_INT
#define MAX_INT  0x7fffffff
#endif

#ifndef TRUE
#define TRUE  1
#define FALSE 0
#endif

typedef int ID;  

#define OPTIMIZATION

// identifier length 
#define NUM_BITS     24  

// average latency of a packet between any two nodes in the system.
// latency is exponentially distributed 
#define AVG_PKT_DELAY    50  /* ms */

// average interval between two invocation of the
// process_request() procedure. The duration of this interval is
// uniformly distributed in [0.5*PROC_REC_PERIOD, 1.5*PROC_REC_PERIOD)
#define PROC_REQ_PERIOD  500  /* ms */

// average interval between two invocations of the stabilization()
// procedure. The duration of this interval is
// uniformly distributed in [STABILIZDE_REC_PERIOD, 1.5*STABILIZE_PERIOD)
#define STABILIZE_PERIOD 30000  /* ms */

// timeout to detect a node failure. a node eliminates a finger from
// its finger table if it doesn't hear an answer within TIME_OUT ms
// after it has sent a query
#define TIME_OUT        500 /* ms */

// maximum simulation time 
#define MAX_TIME     5e+07 /* in ms */   

// size of hash table used to maintain all nodes
#define HASH_SIZE        100000 

// calendar queue parameters: number of entries and the time interval
// covered by an entry (in event.c)
#define MAX_NUM_ENTRIES  4096
#define ENTRY_TUNIT 100 /* ms */ 

// maximum number of documents in the system
#define MAX_NUM_DOCS    100000

// finger table size (see pseudocode.txt)
#define MAX_NUM_FINGERS 40
// number of successors maintained by each node; these are the first
// NUM_SUCC nodes in the finger table
#define NUM_SUCCS 20

// display some debugging  info
// #define TRACE

#define MAX_CMD_SIZE 128

#endif 





