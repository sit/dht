/* Copyright (c) 2003 Lucent Technologies. See LICENSE. */

#include "taskimpl.h"

static void
launcher386(void (*f)(void *arg), void *arg)
{
	(*f)(arg);
	taskexit(0);
}

void
tinitstack(Task *t, void (*f)(void*), void *arg)
{
	ulong *tos;
#if 0
	memset(t->stack, 0xFE, t->stacksize);
#endif
	tos = (ulong*)(t->stack+t->stacksize);
	*--tos = (ulong)arg;
	*--tos = (ulong)f;
	*--tos = 0;	/* return PC */
	t->sched.r[0] = (ulong)launcher386;
	t->sched.r[2] = (ulong)tos - 4;		/* ??? */
}

