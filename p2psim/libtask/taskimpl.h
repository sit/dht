/* Copyright (c) 2005 Russ Cox, MIT; see COPYRIGHT */

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sched.h>
#include <signal.h>
#include <ucontext.h>
#include <sys/utsname.h>
#include "task.h"

#define nil ((void*)0)
#define nelem(x) (sizeof(x)/sizeof((x)[0]))

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

#if defined(__FreeBSD__) && __FreeBSD_version < 500000
extern	int		getcontext(ucontext_t*);
extern	void		setcontext(ucontext_t*);
extern	int		swapcontext(ucontext_t*, ucontext_t*);
extern	void		makecontext(ucontext_t*, void(*)(), int, ...);
#endif

#if defined(__APPLE__)
#	define mcontext libthread_mcontext
#	define mcontext_t libthread_mcontext_t
#	define ucontext libthread_ucontext
#	define ucontext_t libthread_ucontext_t
	typedef struct mcontext mcontext_t;
	typedef struct ucontext ucontext_t;
	struct mcontext
	{
		ulong	pc;		/* lr */
		ulong	cr;		/* mfcr */
		ulong	ctr;		/* mfcr */
		ulong	xer;		/* mfcr */
		ulong	sp;		/* callee saved: r1 */
		ulong	toc;		/* callee saved: r2 */
		ulong	r3;		/* first arg to function, return register: r3 */
		ulong	gpr[19];	/* callee saved: r13-r31 */
	// XXX: currently do not save vector registers or floating-point state
	//	ulong	pad;
	//	uvlong	fpr[18];	/* callee saved: f14-f31 */
	//	ulong	vr[4*12];	/* callee saved: v20-v31, 256-bits each */
	};
	
	struct ucontext
	{
		struct {
			void *ss_sp;
			uint ss_size;
		} uc_stack;
		sigset_t uc_sigmask;
		mcontext_t mc;
	};
	
	void makecontext(ucontext_t*, void(*)(void), int, ...);
	int getcontext(ucontext_t*);
	int setcontext(ucontext_t*);
	int swapcontext(ucontext_t*, ucontext_t*);
	int _getmcontext(mcontext_t*);
	void _setmcontext(mcontext_t*);
	
#endif

typedef struct Context Context;

enum
{
	STACK = 8192
};

struct Context
{
	ucontext_t	uc;
};

struct Task
{
	Task	*next;
	Task	*prev;
	Task	*allnext;
	Task	*allprev;
	Context	context;
	uint	id;
	uchar	*stk;
	uint	stksize;
	int		exiting;
	void	(*startfn)(void*);
	void	*startarg;
	char	name[256];
	char	state[256];
	void *udata;
};

void	taskready(Task*);
void	taskswitch(void);

extern Task *taskrunning;

