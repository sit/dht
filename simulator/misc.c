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


#include "incl.h"
#include <stdlib.h>

// compute (id + i) mod 2^NUM_BITS 
int successorId(int id, int i)
{
  //  id = id + (1 << i);
  id = id + i;

  if (id >= (1 << NUM_BITS))
    id -= (1 << NUM_BITS);

  return id;
}

// compute (id - i) mod 2^NUM_BITS 
int predecessorId(int id, int i)
{
  // id = id - (1 << i);
  id = id - i;

  if (id < 0)
    id += (1 << NUM_BITS);

  return id;
}

// check whether x in (a, b) on the circle 
int between(int x, int a, int b)
{
  if (a == b || a == x || x == b)
    return FALSE;
  else if (a < x && x < b)
    return TRUE;
  else if ((b < a) && (b > x || x > a))
    return TRUE;

  return FALSE;
}

int *newInt(int val) 
{ 
  int *p;

  if (!(p = (int *)malloc(sizeof(int))))
    panic("newInt: memory alloc. error.\n");
  
  *p = val;

  return p;
}


