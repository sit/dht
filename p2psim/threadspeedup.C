#include <lib9.h>

typedef struct Proc Proc;


extern "C" {

/*
 * This is only safe because this is a single-process program.
 */
int
_threadgetpid(void)
{
	return 0;
}

/*
 * This is used to clear the stacks with 0xFE before using
 * them.   It helps debugging but isn't necessary.
 */

void
_threaddebugmemset(void *v, int c, int n)
{
}

/*
 * This only works because there's one proc.
 */
static Proc *p;

Proc*
_threadgetproc(void)
{
	return p;
}

Proc*
_threaddelproc(void)
{
	return p;
}

void
_threadsetproc(Proc *pp)
{
	p = pp;
}


/*
 * Disable debugging prints entirely.
 */
void
_threaddebug(ulong x, char *fmt, ...)
{
}


#ifndef THESE_DONT_MAKE_A_DIFFERENCE
/*
 * This is only safe because we're running in a single process.
 */
int
_tas(void *v)
{
	int z;

	z = *(int*)v;
	*(int*)v = 1;
	return z;
}


void
lock(Lock *l)
{
}

void
unlock(Lock *l)
{
}

int
canlock(Lock *l)
{
	return 1;
}

#endif



}
