/*
 * Copyright (c) 2003 Lucent Technologies.
 * Copyright (c) 2003, 2004 Russ Cox.
 * See LICENSE.
 */
#include "taskimpl.h"
#include <signal.h>

static Mach xm;
Mach *taskmach;
static Rgrp rgrp;
Task *tidle;

static char *psstate[] = {
	"Dead",
	"Running",
	"Ready",
	"Rendezvous"
};

int
anyready(void)
{
	return taskmach->ready.head != nil;
}

void
schedinit(void)
{
	Task *t;

again:
	if(taskmach->tasks.head == nil)
		exit(taskmach->exitstatus);
	while(taskmach->ready.head == nil){
		taskmach->t = tidle;
		idle();
		taskmach->t = nil;
	}
	t = tqpop(&taskmach->ready);
	if(t->moribund)
		gotolabel(&taskmach->sched);
	t->state = Running;
	t->nextstate = Ready;
	taskmach->t = t;

	if(setlabel(&taskmach->sched) == 0)
		gotolabel(&t->sched);

	if(t->moribund){
		/* fprint(2, "%d: %s [dying] %s\n", t->id, t->name, t->statestr); */
		assert(t->moribund == 1);
		t->state = Dead;
		tqremove(&taskmach->tasks, t);
		tfree(t);
	}else{
		t->state = t->nextstate;
		if(t->state == Ready)
			ready(t);
	}
	taskmach->t = nil;
	goto again;
}

void
sched(void)
{
	Task *t;

	t = taskmach->t;
	assert(t != tidle);
	if((ulong)&t < (ulong)t){
		fprint(2, "stack overflow\n");
		abort();
	}
#if 0
	if (*(unsigned int *)(t->stack)!=0xFEFEFEFE){
	  fprint(2, "stack overflow fence %x %x\n", (ulong)&t, (ulong)t->stack);
	  abort();
	}
#endif 

//fprint(2, "%d: sched ...\n", t->id);
	if(setlabel(&t->sched)==0) {
		if (((((ulong )taskmach->sched.r[0]))>0x080480ac)&& (((ulong )taskmach->sched.r[0])<0x0813168d)) {
		  gotolabel(&taskmach->sched);
		}else{
		  fprintf(2,"eip is crap %x\n", (((ulong )taskmach->sched.r[0])));
		  abort();
		}
	}
//fprint(2, "%d: endsched ...\n", t->id);
}

void
ready(Task *t)
{
//fprint(2, "%d: ready %d ...\n", taskmach->t ? taskmach->t->id : 0, t->id);
	tqappend(&taskmach->ready, t);
//fprint(2, "%d: endready\n");
}

Task*
talloc(uint stacksize)
{
	Task *t;
	static int taskid;

	t = emalloc(sizeof(Task)+stacksize);
	t->stack = (char*)(t+1);
	t->stacksize = stacksize;
	t->magic = THREADMAGIC;
	t->moribund = 0;
	t->state = 0;
	t->id = ++taskid;
	return t;
}

void
tfree(Task *t)
{
#if 0
	if (*(unsigned int *)(t->stack)!=0xFEFEFEFE){
	  fprint(2, "stack overflow fence %x %x\n", (ulong)&t, (ulong)t->stack);
	  abort();
	}
#endif
	t->magic = 0;
	free(t);
}

void
yield(void)
{
	assert(taskmach->t != tidle);
	sched();
}

int
taskcreate(void (*f)(void *arg), void *arg, uint stacksize)
{
	Task *t;

	t = talloc(stacksize);
	tinitstack(t, f, arg);
	t->id = ++taskmach->tid;
	tqappend(&taskmach->tasks, t);
	ready(t);
	return t->id;
}

void
taskexit(int status)
{
	taskstate("exit %d", status);
	taskmach->t->moribund = 1;
	taskmach->exitstatus = status;
	sched();
}

void
taskexitall(int status)
{
	exit(status);
}

ulong
taskrendezvous(ulong tag, ulong val)
{
	ulong ret;
	Task *t, **l;

	l = &rgrp.hash[tag%nelem(rgrp.hash)];
	for(t=*l; t; l=&t->rendhash, t=*l)
		if(t->rendtag == tag)
			goto send;

/* recv: */
	t = taskmach->t;
	t->rendtag = tag;
	t->rendval = val;
	t->rendhash = *l;
	*l = t;
	t->nextstate = Rendezvous;
//fprint(2, "%d: rendezvous %lux %lux ...\n", t->id, tag, val);
	sched();
//fprint(2, "%d: endrendezvous %lux\n", t->id, t->rendval);
	return t->rendval;

send:
//fprint(2, "%d: rendezvous %lux %lux with %d: %lux\n",
//	taskmach->t->id, tag, val, t->id, t->rendval);
//fprint(2, "%d: end rendezvous\n", taskmach->t->id);
	*l = t->rendhash;
	ret = t->rendval;
	t->rendval = val;
	ready(t);
	return ret;
}

void**
taskdata(void)
{
	return &taskmach->t->udata;
}

uint
taskid(void)
{
	return taskmach->t->id;
}

static void
mainjumper(void *v)
{
	int *p;

	p = v;
	taskmain(p[0], (char**)p[1]);
}

void
tasksetname(char *s)
{
	Task *t;

	t = taskmach->t;
	strecpy(t->name, t->name+sizeof t->name, s);
}

void
taskstate(char *fmt, ...)
{
	Task *t;
	va_list arg;

	t = taskmach->t;
	if(fmt[0] == 0){
		char buf[sizeof t->statestr];
		if(memcmp(t->statestr, "WAS", 3) == 0)
			return;
		strcpy(buf, t->statestr);
		snprint(t->statestr, sizeof t->statestr, "WAS %s", buf);
		return;
	}
	if(t->statestr[0] && memcmp(t->statestr, "WAS", 3) != 0)
		return;
	va_start(arg, fmt);
	vsnprint(t->statestr, sizeof t->statestr, fmt, arg);
	va_end(arg);
}

void
taskdump(void)
{
	Task *t;

	for(t=taskmach->tasks.head; t; t=t->qall.next)
		print("%d %s [%s] %s\n", t->id, t->name, psstate[t->state], t->statestr);
}

void
siginfo(int x)
{
	taskdump();
}

#undef	offsetof
#define	offsetof(s, m)	(ulong)(&(((s*)0)->m))
int
main(int argc, char **argv)
{	
	taskmach = &xm;
	/*signal(SIGINFO, siginfo);*/
	taskmach->ready.tqslot = offsetof(Task, qready);
	taskmach->tasks.tqslot = offsetof(Task, qall);
	taskmach->free.tqslot = offsetof(Task, qfree);

	tidle = talloc(8192);
	taskcreate(mainjumper, &argc, 8192);
	schedinit();
	return 0;
}
