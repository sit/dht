/* Copyright (c) 2003 Russ Cox.  See LICENSE. */

#include "taskimpl.h"

#define NEXT(q, t)	(((Tqslot*)((ulong)(t)+(q)->tqslot))->next)
#define PREV(q, t)	(((Tqslot*)((ulong)(t)+(q)->tqslot))->prev)

Task*
tqpop(Tqueue *q)
{
	Task *t, *tn;

	t = q->head;
	if(t == nil)
{
//fprint(2, "tqpop %p/%d = nil\n", q, q->tqslot);
		return nil;
}
	tn = NEXT(q, t);
	q->head = tn;
	if(tn)
		PREV(q, tn) = nil;
//fprint(2, "tqpop %p = %p\n", q, t);
assert(q->head != t);
	return t;
}

void
tqappend(Tqueue *q, Task *t)
{
//fprint(2, "tqappend %p/%d %p\n", q, q->tqslot, t);
	NEXT(q, t) = PREV(q, t) = nil;
	if(q->head == nil){
		q->head = q->tail = t;
		return;
	}
	NEXT(q, q->tail) = t;
	PREV(q, t) = q->tail;
	q->tail = t;
}

void
tqremove(Tqueue *q, Task *t)
{
	Task *tn, *tp;

//fprint(2, "tqremove %p/%d %p\n", q, q->tqslot, t);
	tn = NEXT(q, t);
	tp = PREV(q, t);
	if(tn)
		PREV(q, tn) = tp;
	else
		q->tail = tp;
	if(tp)
		NEXT(q, tp) = tn;
	else
		q->head = tn;
}

