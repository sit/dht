/* Copyright (c) 2003 Russ Cox.  See LICENSE. */

#include "taskimpl.h"

void*
emalloc(ulong n)
{
	void *v;

	v = malloc(n);
	if(v == 0){
		fprint(2, "out of memory\n");
		abort();
	}
	memset(v, 0, n);
	return v;
}
