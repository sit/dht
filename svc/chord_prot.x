
/*
 * This file was written by Frans Kaashoek.  Its contents is
 * uncopyrighted and in the public domain.  Of course, standards of
 * academic honesty nonetheless prevent anyone in research from
 * falsely claiming credit for this work.
 */

%#include "bigint.h"
%#include <sfs_prot.h>

typedef bigint chordID;

enum chordstat {
  CHORD_OK = 0,
  CHORD_ERRNOENT = 1,
  CHORD_RPCFAILURE = 2,
  CHORD_INRANGE = 3,
  CHORD_NOTINRANGE = 4,
  CHORD_NOHANDLER = 5,
  CHORD_UNKNOWNNODE = 6
};

struct chord_vnode {
  chordID n;
};

struct net_address {
  sfs_hostname hostname;
  int32_t port;
};

struct chord_noderesok {
  chordID node;
  net_address r;
};

union chord_noderes switch (chordstat status) {
 case CHORD_OK:
   chord_noderesok resok;
 default: 
   void;
};

struct chord_findarg {
  chord_vnode v;
  chordID x;
};

struct chord_node {
  chordID x;
  net_address r;
};

struct chord_nodearg {
  chord_vnode v;
  chord_node n;
};

struct chord_cachearg {
  chord_vnode v;
  chordID key;
  chordID node;
};

struct chord_testandfindarg {
  chord_vnode v;
  chordID x;
};

union chord_testandfindres switch (chordstat status) {
 case CHORD_INRANGE:
   chord_node inres;
 case CHORD_NOTINRANGE:
   chord_noderesok noderes;
 default:
   void;
};

struct chord_getfingersresok {
  chord_node fingers<>;
};

union chord_getfingersres switch (chordstat status) {
  case CHORD_OK:
    chord_getfingersresok resok;
  default:
    void;
};

struct chord_challengearg {
  chord_vnode v;
  int challenge;
};

struct chord_challengeresok {
  int challenge;
  int index;
};

union chord_challengeres switch (chordstat status) {
 case CHORD_OK:
   chord_challengeresok resok;
 default:
   void;
};

struct chord_RPC_arg {
  chord_vnode v;
  unsigned host_prog;
  unsigned host_proc;
  opaque marshalled_args<>;
};

struct chord_RPC_resok {
  opaque marshalled_res<>;
};

union chord_RPC_res switch (chordstat status) {
 case CHORD_OK:
   chord_RPC_resok resok;
 default:
   void;
};

program CHORD_PROGRAM {
	version CHORD_VERSION {
		void 
		CHORDPROC_NULL (chord_vnode) = 0;

		chord_noderes 
		CHORDPROC_GETSUCCESSOR (chord_vnode) = 1;

		chord_noderes 
		CHORDPROC_GETPREDECESSOR (chord_vnode) = 2;

	  	chord_noderes
		CHORDPROC_FINDCLOSESTPRED (chord_findarg) = 3;

		chordstat
		CHORDPROC_NOTIFY (chord_nodearg) = 4;

		chordstat
		CHORDPROC_ALERT (chord_nodearg) = 5;

		chordstat
        	CHORDPROC_CACHE (chord_cachearg) = 6;

		chord_testandfindres
                CHORDPROC_TESTRANGE_FINDCLOSESTPRED (chord_testandfindarg) = 7; 

		chord_getfingersres
		CHORDPROC_GETFINGERS (chord_vnode) = 8;
		
		chord_challengeres 
	        CHORDPROC_CHALLENGE (chord_challengearg) = 9;

		chord_RPC_res
		CHORDPROC_HOSTRPC (chord_RPC_arg) = 10;
	} = 1;
} = 344447;
