/* Copyright (c) 2005 Russ Cox, MIT; see COPYRIGHT */

#include "taskimpl.h"

#if defined(__APPLE__)
void
makecontext(ucontext_t *ucp, void (*func)(void), int argc, ...)
{
	ulong *sp, *tos;
	va_list arg;

	tos = (ulong*)ucp->uc_stack.ss_sp+ucp->uc_stack.ss_size/sizeof(ulong);
	sp = tos - 16;	
	ucp->mc.pc = (long)func;
	ucp->mc.sp = (long)sp;
	va_start(arg, argc);
	ucp->mc.r3 = va_arg(arg, long);
	va_end(arg);
}

int
getcontext(ucontext_t *uc)
{
	return _getmcontext(&uc->mc);
}

int
setcontext(ucontext_t *uc)
{
	_setmcontext(&uc->mc);
	return 0;
}

int
swapcontext(ucontext_t *oucp, ucontext_t *ucp)
{
	if(getcontext(oucp) == 0)
		setcontext(ucp);
	return 0;
}
#endif

#if defined(__FreeBSD__) && __FreeBSD_version < 500000
/*
 * FreeBSD 4 and earlier needs the context functions.
 */
void
makecontext(ucontext_t *ucp, void (*func)(void), int argc, ...)
{
	int *sp;

	sp = (int*)ucp->uc_stack.ss_sp+ucp->uc_stack.ss_size/4;
	sp -= argc;
	memmove(sp, &argc+1, argc*sizeof(int));
	*--sp = 0;		/* return address */
	ucp->uc_mcontext.mc_eip = (long)func;
	ucp->uc_mcontext.mc_esp = (int)sp;
}

extern int getmcontext(mcontext_t*);
extern int setmcontext(mcontext_t*);

int
getcontext(ucontext_t *uc)
{
	return getmcontext(&uc->uc_mcontext);
}

void
setcontext(ucontext_t *uc)
{
	setmcontext(&uc->uc_mcontext);
}

int
swapcontext(ucontext_t *oucp, ucontext_t *ucp)
{
	if(getcontext(oucp) == 0)
		setcontext(ucp);
	return 0;
}
#endif

