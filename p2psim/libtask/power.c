/* Copyright (c) 2003 Lucent Technologies.  See LICENSE. */

/* THIS IS UNTESTED */

#include "taskimpl.h"

static void
launcherpower(int a0, int a1, int a2, int a3, int a4, int a5, int a6, int a7,
	void (*f)(void *arg), void *arg)
{
	(*f)(arg);
	taskexit(0);
}

void
tinitstack(Task *t, void (*f)(void*), void *arg)
{
	ulong *tos, *stk;

	tos = (ulong*)&t->stack[t->stacksize&~7];
	stk = tos;
	--stk;
	--stk;
	--stk;
	--stk;
	*--stk = (ulong)arg;
	*--stk = (ulong)f;
	t->sched.pc = (ulong)launcherpower;
	t->sched.sp = (ulong)tos-80;
}

