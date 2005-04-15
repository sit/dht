/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu)
 *                    Robert Morris (rtm@csail.mit.edu),
 *                    Jinyang Li (jinyang@csail.mit.edu)
 *                    Frans Kaashoek (kaashoek@csail.mit.edu).
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef __P2PSIM_H
#define __P2PSIM_H

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include "tmgdmalloc.h"

// multiply per-thread stack size by ...
#define DEFAULT_THREAD_STACKSIZE 8192
#define THREAD_MULTIPLY 2

extern unsigned p2psim_verbose;

#define DEBUG(x) if(p2psim_verbose >= (x)) cout

class Node;

typedef unsigned IPAddress;

// time, in milliseconds
typedef unsigned long long Time;

//
// some utility functions
//

// returns the current time
#define now() (EventQueue::fasttime())

// tries to clean things up cleanly
void graceful_exit(void*);

#ifdef WITH_DMALLOC
# include "dmalloc.h"
#endif

#endif // __P2PSIM_H
