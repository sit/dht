/*
 * This is a clumsy and temporary hack.  I need to find a better
 * way to provide this functionality.  Putting this crap here means
 * I don't have to taint the thread library with this until I figure out
 * the right way to do this.
 *
 * I apologize for this crappy code.  -rsc
 */
#include <lib9.h>

extern "C" {

typedef struct Label Label;
typedef struct Execargs Execargs;
typedef struct Proc Proc;
typedef struct Thread Thread;
typedef struct Tqueue Tqueue;

struct Label
{
	ulong pc;
	ulong bx;
	ulong sp;
	ulong bp;
	ulong si;
	ulong di;
};

struct Execargs
{
	char		*prog;
	char		**args;
	int		fd[2];
};

struct Tqueue		/* Thread queue */
{
	int		asleep;
	Thread	*head;
	Thread	**tail;
};

struct Proc
{
	Lock		lock;
	Label	sched;		/* for context switches */
	Proc		*link;		/* in proctab */
	int		pid;			/* process id */
	int		splhi;		/* delay notes */
	Thread	*thread;		/* running thread */

	int		needexec;
	Execargs	exec;		/* exec argument */
	Proc		*newproc;	/* fork argument */
	char		exitstr[ERRMAX];	/* exit status */

	int		rforkflag;
	int		nthreads;
	Tqueue	threads;		/* All threads of this proc */
	Tqueue	ready;		/* Runnable threads */
};

struct Thread
{
	Lock		lock;			/* protects thread data structure */
	Label	sched;		/* for context switches */
	int		id;			/* thread id */
  /* and other stuff... */
};

extern Proc *_threadgetproc(void);

}

/* are there any threads ready in the current Proc? */
int
anyready()
{
	Proc *p;

	p = _threadgetproc();

	/* this is only safe because we're not using procs yet */
	return p->ready.head != nil;
}
