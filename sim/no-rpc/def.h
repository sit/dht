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

#ifndef MAX_INT
#define MAX_INT  0x7fffffff
#endif

#ifndef TRUE
#define TRUE  1
#define FALSE 0
#endif

typedef int ID;

#define OPTIMIZATION

#define NUM_BITS     24

#define AVG_PKT_DELAY    50  /* in ms */
#define PROC_REQ_PERIOD  500
#define STABILIZE_PERIOD 30000
#define LEAVE_WAIT       (STABILIZE_PERIOD << 1)
#define TIME_OUT        500

/* maximum simulation time */
#define MAX_TIME     5e+07 /* in ms */   

/* used in node.c */
#define POWER_HASH_TABLE 10
#define HASH_SIZE        100000

/* used in event.c */
#define MAX_NUM_ENTRIES  4096
#define ENTRY_TUNIT 100 /* msec */ 

#define MAX_NUM_DOCS    1000
#define MAX_NUM_FINGERS 40
#define NUM_SUCCS 20

#endif 





