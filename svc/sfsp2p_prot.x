
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

struct route {
  sfs_hostname server;
  int port;
};

struct mapping {
  sfs_ID x;
  route r;
};

struct sfsp2p_findresok {
  sfs_ID x;
  sfs_ID node;
  route r;
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
  route r;
};


struct sfsp2p_movearg {
   sfs_ID x;
};

struct sfsp2p_moveresok {
   mapping mappings<>;
};

union sfsp2p_moveres switch (sfsp2pstat status) {
 case SFSP2P_OK:
   sfsp2p_moveresok resok;
 default: 
   void;
};

struct sfsp2p_insertarg {
  sfs_ID d;
  route r;
};

struct sfsp2p_lookupresok {
  route r;
};

union sfsp2p_lookupres switch (sfsp2pstat status) {
 case SFSP2P_OK:
   sfsp2p_lookupresok resok;
 default: 
   void;
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

		sfsp2p_moveres
		SFSP2PPROC_MOVE (sfsp2p_movearg) = 7;

		sfsp2pstat
		SFSP2PPROC_INSERT (sfsp2p_insertarg) = 8;

		sfsp2p_lookupres
		SFSP2PPROC_LOOKUP (sfs_ID) = 9;

	} = 1;
} = 344447;

program SFSP2PCLNT_PROGRAM {
	version SFSP2PCLNT_VERSION {

		void 
		SFSP2PCLNTPROC_NULL (void) = 0;

		sfsp2p_lookupres
		SFSP2PCLNTPROC_LOOKUP (sfs_ID) = 1;

		sfsp2pstat
		SFSP2PCLNTPROC_INSERT (sfsp2p_insertarg) = 2;
	} = 1;
} = 344448;

