/* Copyright (c) 2003 Russ Cox.  See LICENSE. */

#include "taskimpl.h"

/* THIS IS UNTESTED */

static void
launchersparc(uint o0, uint o1, uint o2, uint o3,
	uint o4, uint o5, uint o6, uint o7,
	void (*f)(void *arg), void *arg)
{
	if(0) print("ls %x %x %x %x %x %x %x %x %x %x at %x\n",
		o0, o1, o2, o3, o4, o5, o6, o7, f, arg, &o0);
	(*f)(arg);
	taskexit(0);
}

void
tinitstack(Task *t, void (*f)(void*), void *arg)
{
	ulong *tos;

	/*
	 * This is a bit more complicated than it should be,
	 * because we need to set things up so that gotolabel
	 * (which executes a return) gets us into launchersparc.
	 * So all the registers are going to be renamed before
	 * we get there.  The input registers here become the
	 * output registers there, which is useless.  
	 * The input registers there are inaccessible, so we
	 * have to give launchersparc enough arguments that
	 * everything ends up in the stack.
	 */
	tos = (ulong*)((uchar*)t->stack + t->stacksize&~7);
	--tos;
	*--tos = (ulong)arg;
	*--tos = (ulong)f;
	tos -= 25;	/* would love to understand this */
	t->sched.link = (ulong)launchersparc - 8;
	t->sched.input[6] = 0;
	t->sched.sp = (ulong)tos;
	if(0) print("tis %x %x at %x\n", f, arg, t->sched.sp);
}

