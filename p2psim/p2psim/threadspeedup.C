/*
 * Copyright (c) 2003 [NAMES_GO_HERE]
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
 */

#include <lib9.h>

typedef struct Proc Proc;


extern "C" {

/*
 * This is only safe because this is a single-process program.
 */
int
_threadgetpid(void)
{
	return 0;
}

/*
 * This is used to clear the stacks with 0xFE before using
 * them.   It helps debugging but isn't necessary.
 */

void
_threaddebugmemset(void *v, int c, int n)
{
}

/*
 * This only works because there's one proc.
 */
static Proc *p;

Proc*
_threadgetproc(void)
{
	return p;
}

Proc*
_threaddelproc(void)
{
	return p;
}

void
_threadsetproc(Proc *pp)
{
	p = pp;
}


/*
 * Disable debugging prints entirely.
 */
void
_threaddebug(ulong x, char *fmt, ...)
{
}


#ifndef THESE_DONT_MAKE_A_DIFFERENCE
/*
 * This is only safe because we're running in a single process.
 */
int
_tas(void *v)
{
	int z;

	z = *(int*)v;
	*(int*)v = 1;
	return z;
}


void
lock(Lock *l)
{
}

void
unlock(Lock *l)
{
}

int
canlock(Lock *l)
{
	return 1;
}

#endif



}
