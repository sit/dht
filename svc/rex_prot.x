/* $Id: rex_prot.x,v 1.1 2001/01/18 16:21:51 fdabek Exp $ */

/*
 * This file was written by David Mazieres.  Its contents is
 * uncopyrighted and in the public domain.  Of course, standards of
 * academic honesty nonetheless prevent anyone in research from
 * falsely claiming credit for this work.
 */

%#include "sfs_prot.h"

typedef string ttypath<63>;
typedef string utmphost<16>;

/* Note, a successful PTYD_PTY_ALLOC result is accompanied by a file
 * descriptor for the master side of the pty. */
union pty_alloc_res switch (int err) {
 case 0:
   ttypath path;
 default:
   void;
};

program PTYD_PROG {
	version PTYD_VERS {
		void
		PTYD_NULL (void) = 0;

		pty_alloc_res
		PTYD_PTY_ALLOC (utmphost) = 1;

		int
		PTYD_PTY_FREE (ttypath) = 2;
	} = 1;
} = 344431;

typedef string rex_progarg<>;
typedef rex_progarg rex_cmd<>;

struct rexd_spawn_arg {
  sfs_kmsg kmsg;
  rex_cmd command;
};

struct rexd_spawn_resok {
  sfs_kmsg kmsg;
};

union rexd_spawn_res switch (sfsstat err) {
 case SFS_OK:
   rexd_spawn_resok resok;
 default:
   void;
};

struct rexd_attach_arg {
  sfs_hash sessid;
  sfs_seqno seqno;
  sfs_hash newsessid;
};

#if 0
struct rexd_attach_resok {
};

union rexd_attach_res switch (sfsstat err) {
 case SFS_OK:
   rexd_attach_resok resok;
 default:
   void;
};
#else
typedef sfsstat rexd_attach_res;
#endif

struct rex_sesskeydat {
  sfs_msgtype type;		/* = SFS_KSC or SFS_KCS */
  sfs_seqno seqno;
  sfs_secret sshare;		/* Server's share of session key */
  sfs_secret cshare;		/* Client's share of session key */
};

program REXD_PROG {
	version REXD_VERS {
		void
		REXD_NULL (void) = 0;

		/* Must be accompanied by a previously negotiated authuint */
		rexd_spawn_res
		REXD_SPAWN (rexd_spawn_arg) = 1;

		rexd_attach_res
		REXD_ATTACH (rexd_attach_arg) = 2;
	} = 1;
} = 344424;

program REXCTL_PROG {
	version REXCTL_VERS {
		void
		REXCTL_NULL (void) = 0;

		/* REXCTL_CONNECT is preceeded by a file descriptor */
		void
		REXCTL_CONNECT (sfs_sessinfo) = 1;
	} = 1;
} = 344426;

struct rex_payload {
  unsigned channel;
  int fd;
  opaque data<>;
};

struct rex_mkchannel_arg {
  int nfds;
  rex_cmd av;
};

struct rex_mkchannel_resok {
  unsigned channel;
};

union rex_mkchannel_res switch (sfsstat err) {
 case SFS_OK:
   rex_mkchannel_resok resok;
 default:
   void;
};

struct rex_int_arg {
  unsigned channel;
  int val;
};

struct rexcb_newfd_arg {
  unsigned channel;
  int fd;
  int newfd;
};

program REX_PROG {
	version REX_VERS {
		void
		REX_NULL (void) = 0;

		bool
		REX_DATA (rex_payload) = 1;

		/* val is fd to close, or -1 to close channel */
		bool
		REX_CLOSE (rex_int_arg) = 2;

		/* val is signal to deliver */
		bool
		REX_KILL (rex_int_arg) = 3;

		rex_mkchannel_res
		REX_MKCHANNEL (rex_mkchannel_arg) = 4;
	} = 1;
} = 344428;

program REXCB_PROG {
	version REXCB_VERS {
		void
		REXCB_NULL (void) = 0;

		bool
		REXCB_DATA (rex_payload) = 1;

		bool
		REXCB_NEWFD (rexcb_newfd_arg) = 2;

		/* val is exit status or -1 for signal */
		void
		REXCB_EXIT (rex_int_arg) = 3;
	} = 1;
} = 344429;
