/*
 * Copyright (c) 2003 Lucent Technologies.
 * Copyright (c) 2003, 2004 Russ Cox.
 * See LICENSE.
 */


#ifndef _TASK_H_
#define _TASK_H_ 1

#ifdef __cplusplus
extern "C" {
#endif

typedef struct Alt	Alt;
typedef struct Channel	Channel;

struct Channel {
	int			s;		// Size of the channel (may be zero)
	unsigned int			f;		// Extraction point (insertion pt: (f + n) % s)
	unsigned int			n;		// Number of values in the channel
	int			e;		// Element size
	int			freed;	// Set when channel is being deleted
	volatile Alt	**qentry;	// Receivers/senders waiting (malloc)
	volatile int	nentry;	// # of entries malloc-ed
	unsigned char		v[1];		// Array of s values in the channel
};

/* Channel operations for alt: */
typedef enum {
	CHANEND,
	CHANSND,
	CHANRCV,
	CHANNOP,
	CHANNOBLK,
} ChanOp;

struct Alt {
	Channel	*c;		/* channel */
	void		*v;		/* pointer to value */
	ChanOp	op;		/* operation */

	/* the next variables are used internally to alt
	 * they need not be initialized
	 */
	Channel	**tag;	/* pointer to rendez-vous tag */
	int		entryno;	/* entry number */
};


int		alt(Alt alts[]);
int		anyready(void);
Channel*	chancreate(int elemsize, int bufsize);
int		chaninit(Channel *c, int elemsize, int elemcnt);
void		chanfree(Channel *c);
int		chanprint(Channel *, char *, ...);
int		nbrecv(Channel *c, void *v);
void*		nbrecvp(Channel *c);
unsigned long		nbrecvul(Channel *c);
int		nbsend(Channel *c, void *v);
int		nbsendp(Channel *c, void *v);
int		nbsendul(Channel *c, unsigned long v);
int		recv(Channel *c, void *v);
void*		recvp(Channel *c);
unsigned long		recvul(Channel *c);
int		send(Channel *c, void *v);
int		sendp(Channel *c, void *v);
int		sendul(Channel *c, unsigned long v);
int		taskcreate(void (*f)(void *arg), void *arg, unsigned int stacksize);
void		taskexit(int);
void		taskexitall(int);
void		taskmain(int argc, char *argv[]);
void		yield(void);
void**	taskdata(void);
void		tasksetname(char*);
unsigned long		taskrendezvous(unsigned long, unsigned long);
unsigned int		taskid(void);

void	taskstate(char*, ...);

#ifdef __cplusplus
}
#endif
#endif

