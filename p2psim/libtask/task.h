/* Copyright (c) 2005 Russ Cox, MIT; see COPYRIGHT */

#ifndef _TASK_H_
#define _TASK_H_ 1

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <unistd.h>	/* prototype yield before we re-#define it */

/*
 * basic procs and threads
 */

typedef struct Task Task;

#define yield taskyield

int		anyready(void);
int		taskcreate(void (*f)(void *arg), void *arg, unsigned int stacksize);
void		taskexit(int);
void		taskexitall(int);
void		taskmain(int argc, char *argv[]);
int		taskyield(void);
void**	taskdata(void);
void		needstack(int);

unsigned long		taskrendezvous(unsigned long, unsigned long);
unsigned int		taskid(void);

/*
 * channel communication
 */
typedef struct Alt Alt;
typedef struct Altarray Altarray;
typedef struct Channel Channel;

enum
{
	CHANEND,
	CHANSND,
	CHANRCV,
	CHANNOP,
	CHANNOBLK,
};

struct Alt
{
	Channel		*c;
	void		*v;
	unsigned int		op;
	Task		*task;
	Alt			*xalt;
};

struct Altarray
{
	Alt			**a;
	unsigned int		n;
	unsigned int		m;
};

struct Channel
{
	unsigned int			bufsize;
	unsigned int			elemsize;
	unsigned char		*buf;
	unsigned int			nbuf;
	unsigned int			off;
	Altarray	asend;
	Altarray	arecv;
	char			*name;
};

#define	alt		chanalt
#define	nbrecv	channbrecv
#define	nbrecvp	channbrecvp
#define	nbrecvul	channbrecvul
#define	nbsend	channbsend
#define	nbsendp	channbsendp
#define	nbsendul	channbsendul
#define	recv		chanrecv
#define	recvp	chanrecvp
#define	recvul	chanrecvul
#define	send		chansend
#define	sendp	chansendp
#define	sendul	chansendul

int		chanalt(Alt *alts);
Channel*	chancreate(int elemsize, int elemcnt);
void		chanfree(Channel *c);
int		chaninit(Channel *c, int elemsize, int elemcnt);
int		channbrecv(Channel *c, void *v);
void*		channbrecvp(Channel *c);
unsigned long		channbrecvul(Channel *c);
int		channbsend(Channel *c, void *v);
int		channbsendp(Channel *c, void *v);
int		channbsendul(Channel *c, unsigned long v);
int		chanrecv(Channel *c, void *v);
void*		chanrecvp(Channel *c);
unsigned long		chanrecvul(Channel *c);
int		chansend(Channel *c, void *v);
int		chansendp(Channel *c, void *v);
int		chansendul(Channel *c, unsigned long v);

#ifdef __cplusplus
}
#endif
#endif

