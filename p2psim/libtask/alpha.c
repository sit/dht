/* Copyright (c) 2001 Russ Cox.  See LICENSE. */

/* THIS IS UNTESTED. */

#include "taskimpl.h"

/*
  setlabel and gotolabel don't save and restore the argument
  registers, so we pass fn and arg on the stack as the seventh and
  eigth arguments to a trampoline.  We could also use the assembler.
*/

static void
launcheralpha(int a0, int a1, int a2, int a3, int a4, int a5,
      void (*f)(void*), void *arg)
{
	(*f)(arg);
	taskexit(0);
}

void
tinitstack(Task *t, void (*f)(void*), void *arg)
{
	ulong *tos;
	int n;

	n = 2*sizeof(ulong);

	tos = (ulong*)(t->stack+t->stacksize);

	*--tos = (ulong)arg;
	*--tos = (ulong)fn;

	t->sched.pc = (ulong)launcheralpha;
	t->sched.pv = t->label.pc;
	t->sched.sp = XXX I DON'T KNOW;

	return;
}
