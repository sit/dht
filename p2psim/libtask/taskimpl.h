/*
 * Copyright (c) 2003 Lucent Technologies.
 * Copyright (c) 2003, 2004 Russ Cox.
 * See LICENSE.
 */
#include <sys/types.h>
#include <inttypes.h>
#include <assert.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#include "task.h"

#define nil ((void*)0)

#define ulong task_ulong
#define uint task_uint
#define uchar task_uchar
#define ushort task_ushort
#define uvlong task_uvlong
#define vlong task_vlong

typedef unsigned long ulong;
typedef unsigned int uint;
typedef unsigned char uchar;
typedef unsigned short ushort;
typedef unsigned long long uvlong;
typedef long long vlong;

#define print task_print
#define fprint task_fprint
#define snprint task_snprint
#define seprint task_seprint
#define vprint task_vprint
#define vfprint task_vfprint
#define vsnprint task_vsnprint
#define vseprint task_vseprint
#define strecpy task_strecpy

int print(char*, ...);
int fprint(int, char*, ...);
char *snprint(char*, uint, char*, ...);
char *seprint(char*, char*, char*, ...);
int vprint(char*, va_list);
int vfprint(int, char*, va_list);
char *vsnprint(char*, uint, char*, va_list);
char *vseprint(char*, char*, char*, va_list);
char *strecpy(char*, char*, char*);

typedef struct Task Task;
typedef struct Tqueue Tqueue;
typedef struct Tqslot Tqslot;
typedef struct Mach Mach;	/* global state */
typedef struct Rgrp Rgrp;
typedef struct Label Label;

#define nelem(x) (sizeof(x)/sizeof((x)[0]))

enum {
	THREADMAGIC = 0x7644f9ce,
	RENDHASH = 10009,
};

struct Label
{
	ulong r[8];
};

typedef enum
{
	Dead,
	Running,
	Ready,
	Rendezvous,
} State;

typedef enum
{
	Channone,
	Chanalt,
	Chansend,
	Chanrecv,
} Chanstate;

struct Tqueue
{
	int	asleep;
	int	tqslot;
	Task *head;
	Task *tail;
};

struct Tqslot
{
	Task *next;
	Task *prev;
};

struct Task
{
	ulong magic;
	Label sched;
	int id;
	int moribund;
	Tqslot qall;
	Tqslot qready;
	Tqslot qfree;
	Tqslot qlockwait;
	int state;
	int nextstate;
	ulong rendval;
	ulong rendtag;
	Task *rendhash;
	int chan;
	Alt *alt;
	void *udata;
	char statestr[128];
	char name[64];
	char *stack;
	uint stacksize;
};

struct Mach
{
	Tqueue ready;
	Tqueue tasks;
	Tqueue free;
	Task *t;
	int tid;
	int exitstatus;
	int exitsall;
	Label sched;
};

struct Rgrp
{
	Task	*hash[RENDHASH];
};

void	gotolabel(Label*);
int	setlabel(Label*);
void	tfree(Task*);
Task *talloc(unsigned int);
void sched(void);
void schedinit(void);
void ready(Task*);
void tqremove(Tqueue*, Task*);
void tqappend(Tqueue*, Task*);
void idle(void);
Task *tqpop(Tqueue*);
void *emalloc(ulong);
void tinitstack(Task*, void(*)(void*), void*);

extern Mach *taskmach;

