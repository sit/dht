
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

struct net_address {
  sfs_hostname hostname;
  int32_t port;
};

struct chord_node {
  chordID x;
  net_address r;
};

union chord_noderes switch (chordstat status) {
 case CHORD_OK:
   chord_node resok;
 default: 
   void;
};

struct chord_nodelistresok {
  chord_node nlist<>;
};

union chord_nodelistres switch (chordstat status) {
 case CHORD_OK:
   chord_nodelistresok resok;
 default:
   void;
};

struct chord_node_ext {
  chordID x;
  net_address r;
  int32_t a_lat;
  int32_t a_var;
  u_int64_t nrpc;
  bool alive;
};

union chord_nodeextres switch (chordstat status) {
 case CHORD_OK:
   chord_node_ext resok;
 default:
   void;
};

struct chord_nodelistextresok {
  chord_node_ext nlist<>;
};

union chord_nodelistextres switch (chordstat status) {
 case CHORD_OK:
   chord_nodelistextresok resok;
 default:
   void;
};

struct chord_findarg {
  chordID v;
  chordID x;
};

struct chord_nodearg {
  chordID v;
  chord_node n;
};

struct chord_testandfindarg {
  chordID v;
  chordID x;
};

union chord_testandfindres switch (chordstat status) {
 case CHORD_INRANGE:
   chord_node inres;
 case CHORD_NOTINRANGE:
   chord_node noderes;
 default:
   void;
};

struct chord_challengearg {
  chordID v;
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

struct chord_gettoes_arg {
  chordID v;
  int32_t level;
};

program CHORD_PROGRAM {
	version CHORD_VERSION {
		void
		CHORDPROC_NULL (chordID) = 0;

		chord_noderes 
		CHORDPROC_GETSUCCESSOR (chordID) = 1;

		chord_noderes 
		CHORDPROC_GETPREDECESSOR (chordID) = 2;

	  	chord_noderes
		CHORDPROC_FINDCLOSESTPRED (chord_findarg) = 3;

		chordstat
		CHORDPROC_NOTIFY (chord_nodearg) = 4;

		chordstat
		CHORDPROC_ALERT (chord_nodearg) = 5;

		chord_nodelistres
        	CHORDPROC_GETSUCCLIST (chordID) = 6;

		chord_testandfindres
                CHORDPROC_TESTRANGE_FINDCLOSESTPRED (chord_testandfindarg) = 7;

 		chord_nodelistres
		CHORDPROC_GETFINGERS (chordID) = 8;

		chord_challengeres 
	        CHORDPROC_CHALLENGE (chord_challengearg) = 9;

		chord_nodeextres
		CHORDPROC_GETPRED_EXT (chordID) = 10;

		chord_nodelistextres
		CHORDPROC_GETFINGERS_EXT (chordID) = 11;

		chord_nodelistextres
		CHORDPROC_GETSUCC_EXT (chordID) = 12;

		chord_nodelistextres
          	CHORDPROC_GETTOES (chord_gettoes_arg) = 13;
	} = 1;
} = 344447;
