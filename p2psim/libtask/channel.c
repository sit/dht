/* Copyright (c) 2003 Lucent Technologies */

#include "taskimpl.h"

static void enqueue(Alt*, Channel**);
static void dequeue(Alt*);
static int altexec(Alt*);

static int
canexec(Alt *a)
{
	int i, otherop;
	Channel *c;

	c = a->c;
	/* are there senders or receivers blocked? */
	otherop = (CHANSND+CHANRCV) - a->op;
	for(i=0; i<c->nentry; i++)
		if(c->qentry[i] && c->qentry[i]->op==otherop && *c->qentry[i]->tag==nil)
			return 1;

	/* is there room in the channel? */
	if((a->op==CHANSND && c->n < c->s)
	|| (a->op==CHANRCV && c->n > 0))
		return 1;

	return 0;
}

void
chanfree(Channel *c)
{
	int i, inuse;

	inuse = 0;
	for(i = 0; i < c->nentry; i++)
		if(c->qentry[i])
			inuse = 1;
	if(inuse)
		c->freed = 1;
	else{
		if(c->qentry)
			free(c->qentry);
		free(c);
	}
}

int
chaninit(Channel *c, int elemsize, int elemcnt)
{
	if(elemcnt < 0 || elemsize <= 0 || c == nil)
		return -1;
	c->f = 0;
	c->n = 0;
	c->freed = 0;
	c->e = elemsize;
	c->s = elemcnt;
	return 1;
}

Channel*
chancreate(int elemsize, int elemcnt)
{
	Channel *c;

	if(elemcnt < 0 || elemsize <= 0)
		return nil;
	c = emalloc(sizeof(Channel)+elemsize*elemcnt);
	c->e = elemsize;
	c->s = elemcnt;
	return c;
}

int
alt(Alt *alts)
{
	Alt *a, *xa;
	Channel *volatile c;
	int n;
	ulong r;
	Task *t;

	t = taskmach->t;
	if(t->moribund)
		yield();
	t->alt = alts;
	t->chan = Chanalt;

	taskstate("alt %d", alts[0].op);
	/* test whether any channels can proceed */
	n = 0;
	a = nil;

	for(xa=alts; xa->op!=CHANEND && xa->op!=CHANNOBLK; xa++){
		xa->entryno = -1;
		if(xa->op == CHANNOP)
			continue;
		
		c = xa->c;
		if(c==nil){
			t->chan = Channone;
			taskstate("");
			return -1;
		}
		if(canexec(xa))
			if(rand()%++n == 0)
				a = xa;
	}

	if(a==nil){
		/* nothing can proceed */
		if(xa->op == CHANNOBLK){
			t->chan = Channone;
			taskstate("");
			return xa - alts;
		}

		/* enqueue on all channels. */
		c = nil;
		for(xa=alts; xa->op!=CHANEND; xa++){
			if(xa->op==CHANNOP)
				continue;
			enqueue(xa, (Channel**)&c);
		}

		/*
		 * wait for successful rendezvous.
		 * we can't just give up if the rendezvous
		 * is interrupted -- someone else might come
		 * along and try to rendezvous with us, so
		 * we need to be here.
		 */
	    Again:
		r = taskrendezvous((ulong)&c, 0x98765);

		if(r==~0){		/* interrupted */
			if(c!=nil)		/* someone will meet us; go back */
				goto Again;
			c = (Channel*)~0;	/* so no one tries to meet us */
		}

		/* dequeue from channels, find selected one */
		a = nil;
		for(xa=alts; xa->op!=CHANEND; xa++){
			if(xa->op==CHANNOP)
				continue;
			if(xa->c == c)
				a = xa;
			dequeue(xa);
		}
		if(a == nil){	/* we were interrupted */
			assert(c==(Channel*)~0);
			taskstate("");
			return -1;
		}
	}else{
		altexec(a);	/* unlocks chanlock, does splx */
	}
	t->chan = Channone;
	taskstate("");
	return a - alts;
}

static int
runop(int op, Channel *c, void *v, int nb)
{
	int r;
	Alt a[2];

	/*
	 * we could do this without calling alt,
	 * but the only reason would be performance,
	 * and i'm not convinced it matters.
	 */
	a[0].op = op;
	a[0].c = c;
	a[0].v = v;
	a[1].op = CHANEND;
	if(nb)
		a[1].op = CHANNOBLK;
	switch(r=alt(a)){
	case -1:	/* interrupted */
		return -1;
	case 1:	/* nonblocking, didn't accomplish anything */
		assert(nb);
		return 0;
	case 0:
		return 1;
	default:
		fprint(2, "ERROR: channel alt returned %d\n", r);
		abort();
		return -1;
	}
}

int
recv(Channel *c, void *v)
{
	return runop(CHANRCV, c, v, 0);
}

int
nbrecv(Channel *c, void *v)
{
	return runop(CHANRCV, c, v, 1);
}

int
send(Channel *c, void *v)
{
	return runop(CHANSND, c, v, 0);
}

int
nbsend(Channel *c, void *v)
{
	return runop(CHANSND, c, v, 1);
}

static void
channelsize(Channel *c, int sz)
{
	if(c->e != sz){
		fprint(2, "expected channel with elements of size %d, got size %d",
			sz, c->e);
		abort();
	}
}

int
sendul(Channel *c, ulong v)
{
	channelsize(c, sizeof(ulong));
	return send(c, &v);
}

ulong
recvul(Channel *c)
{
	ulong v;

	channelsize(c, sizeof(ulong));
	if(recv(c, &v) < 0)
		return ~0;
	return v;
}

int
sendp(Channel *c, void *v)
{
	channelsize(c, sizeof(void*));
	return send(c, &v);
}

void*
recvp(Channel *c)
{
	void *v;

	channelsize(c, sizeof(void*));
	if(recv(c, &v) < 0)
		return nil;
	return v;
}

int
nbsendul(Channel *c, ulong v)
{
	channelsize(c, sizeof(ulong));
	return nbsend(c, &v);
}

ulong
nbrecvul(Channel *c)
{
	ulong v;

	channelsize(c, sizeof(ulong));
	if(nbrecv(c, &v) == 0)
		return 0;
	return v;
}

int
nbsendp(Channel *c, void *v)
{
	channelsize(c, sizeof(void*));
	return nbsend(c, &v);
}

void*
nbrecvp(Channel *c)
{
	void *v;

	channelsize(c, sizeof(void*));
	if(nbrecv(c, &v) == 0)
		return nil;
	return v;
}

static int
emptyentry(Channel *c)
{
	int i, extra;

	assert((c->nentry==0 && c->qentry==nil) || (c->nentry && c->qentry));

	for(i=0; i<c->nentry; i++)
		if(c->qentry[i]==nil)
			return i;

	extra = 16;
	c->nentry += extra;
	c->qentry = realloc((void*)c->qentry, c->nentry*sizeof(c->qentry[0]));
	if(c->qentry == nil){
		fprint(2, "realloc channel entries: %r");
		abort();
	}
	memset(&c->qentry[i], 0, extra*sizeof(c->qentry[0]));
	return i;
}

static void
enqueue(Alt *a, Channel **c)
{
	int i;

	a->tag = c;
	i = emptyentry(a->c);
	a->c->qentry[i] = a;
}

static void
dequeue(Alt *a)
{
	int i;
	Channel *c;

	c = a->c;
	for(i=0; i<c->nentry; i++)
		if(c->qentry[i]==a){
			c->qentry[i] = nil;
			if(c->freed)
				chanfree(c);
			return;
		}
}

static void*
altexecbuffered(Alt *a, int willreplace)
{
	uchar *v;
	Channel *c;

	c = a->c;
	/* use buffered channel queue */
	if(a->op==CHANRCV && c->n > 0){
		v = c->v + c->e*(c->f%c->s);
		if(!willreplace)
			c->n--;
		c->f++;
		return v;
	}
	if(a->op==CHANSND && c->n < c->s){
		v = c->v + c->e*((c->f+c->n)%c->s);
		if(!willreplace)
			c->n++;
		return v;
	}
	abort();
	return nil;
}

static void
altcopy(void *dst, void *src, int sz)
{
	if(dst){
		if(src)
			memmove(dst, src, sz);
		else
			memset(dst, 0, sz);
	}
}

static int
altexec(Alt *a)
{
	volatile Alt *b;
	int i, n, otherop;
	Channel *c;
	void *me, *waiter, *buf;

	c = a->c;

	/* rendezvous with others */
	otherop = (CHANSND+CHANRCV) - a->op;
	n = 0;
	b = nil;
	me = a->v;
	for(i=0; i<c->nentry; i++)
		if(c->qentry[i] && c->qentry[i]->op==otherop && *c->qentry[i]->tag==nil)
			if(rand()%++n == 0)
				b = c->qentry[i];
	if(b != nil){
		waiter = b->v;
		if(c->s && c->n){
			/*
			 * if buffer is full and there are waiters
			 * and we're meeting a waiter,
			 * we must be receiving.
			 *
			 * we use the value in the channel buffer,
			 * copy the waiter's value into the channel buffer
			 * on behalf of the waiter, and then wake the waiter.
			 */
			if(a->op!=CHANRCV)
				abort();
			buf = altexecbuffered(a, 1);
			altcopy(me, buf, c->e);
			altcopy(buf, waiter, c->e);
		}else{
			if(a->op==CHANRCV)
				altcopy(me, waiter, c->e);
			else
				altcopy(waiter, me, c->e);
		}
		*b->tag = c;	/* commits us to rendezvous */
		while(taskrendezvous((ulong)b->tag, 0x54321) == ~0)
			;
		return 1;
	}

	buf = altexecbuffered(a, 0);
	if(a->op==CHANRCV)
		altcopy(me, buf, c->e);
	else
		altcopy(buf, me, c->e);

	return 1;
}
