/* $Id: nfsmounter.x,v 1.1 2001/01/18 16:21:51 fdabek Exp $ */

%#ifndef NMOPT_ONLY

%#include "nfs_prot.h"
%#include "nfs3_prot.h"

%#else /* NMOPT_ONLY */

%#undef NFS3_FHSIZE;
const NFS3_FHSIZE = 64;

%#endif /* NMOPT_ONLY */

const NMOPT_RO   = 0x1;		/* Mount read-only */
const NMOPT_SOFT = 0x2;		/* Mount soft */
const NMOPT_NOAC = 0x4;		/* Disable attribute cache */
const NMOPT_NFS3 = 0x8;		/* Enable NFS version 3 */
const NMOPT_RDPLUS = 0x10;	/* Use NFS3 readdirplus */

typedef opaque nfsmnt_handle<NFS3_FHSIZE>;

%#ifndef NMOPT_ONLY

struct mountarg {
  string hostname<>;
  string path<>;
  int flags;
  nfsmnt_handle handle;
};

union mountres switch (int status) {
 case 0:
   uint64 fsid;
 default:
   void;
};

struct remountarg {
  string path<>;
  int flags;
};

%#endif /* !NMOPT_ONLY */

const NUOPT_FORCE = 0x1;	/* Force unmount */
const NUOPT_STALE = 0x2;	/* Serve stale NFS file system */
const NUOPT_NOOP = 0x4;		/* Don't actually call unmount */
const NUOPT_NLOG = 0x8;		/* Don't log failed unmount */

%#ifndef NMOPT_ONLY

struct umountarg {
  string path<>;
  int flags;
};

/* Note:  NFSMOUNTER_MOUNT and NFSMOUNTER_CLOSE must me preceeded by
 * the UDP file descriptor of the NFS server (sent via
 * axprt_unix::sendfd).  */

program NFSMOUNTER_PROG {
	version NFSMOUNTER_VERS {
		void NFSMOUNTER_NULL (void) = 0;
		mountres NFSMOUNTER_MOUNT (mountarg) = 1;
		int NFSMOUNTER_REMOUNT (remountarg) = 2;
		int NFSMOUNTER_UMOUNT (umountarg) = 3;
		int NFSMOUNTER_UMOUNTALL (int /* flags */) = 4;
	} = 1;
} = 395544;

%#endif /* !NMOPT_ONLY */
