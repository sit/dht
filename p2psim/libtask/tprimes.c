/* Copyright (c) 2005 Russ Cox, MIT; see COPYRIGHT */

#include "taskimpl.h"

int quiet;
int goal;
int buffer;

void
primetask(void *arg)
{
	Channel *c, *nc;
	int p, i;
	c = arg;

	p = recvul(c);
	if(p > goal)
		taskexitall(0);
	if(!quiet)
		print("%d\n", p);
	nc = chancreate(sizeof(unsigned long), buffer);
	taskcreate(primetask, nc, 8192);
	for(;;){
		i = recvul(c);
		if(i%p)
			sendul(nc, i);
	}
}

void
taskmain(int argc, char **argv)
{
	int i;
	Channel *c;

	if(argc>1)
		goal = atoi(argv[1]);
	else
		goal = 100;
print("taskmain goal=%d\n", goal);

	c = chancreate(sizeof(unsigned long), buffer);
	taskcreate(primetask, c, 8192);
	for(i=2;; i++)
		sendul(c, i);
}

void*
emalloc(unsigned long n)
{
	return calloc(n ,1);
}

long
lrand(void)
{
	return rand();
}
