
/*
 * This file was written by Frans Kaashoek.  Its contents is
 * uncopyrighted and in the public domain.  Of course, standards of
 * academic honesty nonetheless prevent anyone in research from
 * falsely claiming credit for this work.
 */

%#include "bigint.h"
%#include <sfs_prot.h>

typedef bigint sfs_ID;

enum sfsp2pstat {
  SFSP2P_OK = 0,
  SFSP2P_ERRNOENT = 1,
  SFSP2P_RPCFAILURE = 2,
  SFSP2P_ERREXIST = 3
};

struct net_address {
  sfs_hostname hostname;
  int32_t port;
};

struct sfsp2p_findresok {
  sfs_ID x;
  sfs_ID node;
  net_address r;
};

union sfsp2p_findres switch (sfsp2pstat status) {
 case SFSP2P_OK:
   sfsp2p_findresok resok;
 default: 
   void;
};

struct sfsp2p_findarg {
  sfs_ID x;
};

struct sfsp2p_notifyarg {
  sfs_ID x;
  net_address r;
};

struct sfsp2p_cachearg {
  sfs_ID key;
  sfs_ID node;
};

program SFSP2P_PROGRAM {
	version SFSP2P_VERSION {
		void 
		SFSP2PPROC_NULL (void) = 0;

		sfsp2p_findres 
		SFSP2PPROC_GETSUCCESSOR (void) = 1;
		sfsp2p_findres 

		SFSP2PPROC_GETPREDECESSOR (void) = 2;

		sfsp2p_findres
		SFSP2PPROC_FINDCLOSESTSUCC (sfsp2p_findarg) = 3;

	  	sfsp2p_findres
		SFSP2PPROC_FINDCLOSESTPRED (sfsp2p_findarg) = 4;

		sfsp2pstat
		SFSP2PPROC_NOTIFY (sfsp2p_notifyarg) = 5;

		sfsp2pstat
		SFSP2PPROC_ALERT (sfsp2p_notifyarg) = 6;

		sfsp2pstat
        	SFSP2PPROC_CACHE (sfsp2p_cachearg) = 7;
	} = 1;
} = 344447;

program SFSP2PCLNT_PROGRAM {
	version SFSP2PCLNT_VERSION {

		void 
		SFSP2PCLNTPROC_NULL (void) = 0;

		sfs_ID
		SFSP2PCLNTPROC_FINDSUCC (sfs_ID) = 1;

	} = 1;
} = 344448;

