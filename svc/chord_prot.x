
/*
 * This file was written by Frans Kaashoek.  Its contents is
 * uncopyrighted and in the public domain.  Of course, standards of
 * academic honesty nonetheless prevent anyone in research from
 * falsely claiming credit for this work.
 */

%#include "chord_types.h"

enum chordstat {
  CHORD_OK = 0,
  CHORD_ERRNOENT = 1,
  CHORD_RPCFAILURE = 2,
  CHORD_INRANGE = 3,
  CHORD_NOTINRANGE = 4,
  CHORD_NOHANDLER = 5,
  CHORD_STOP = 6
};

union chord_noderes switch (chordstat status) {
 case CHORD_OK:
   chord_node_wire resok;
 default: 
   void;
};

struct chord_nodelistresok {
  chord_node_wire nlist<>;
};

union chord_nodelistres switch (chordstat status) {
 case CHORD_OK:
   chord_nodelistresok resok;
 default:
   void;
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
  chordID x;
};

struct chord_nodearg {
  chord_node_wire n;
};

struct chord_testandfindarg {
  chordID x;
  unsigned upcall_prog;
  unsigned upcall_proc;
  opaque upcall_args<>;
  chordID failed_nodes<>;
};

struct chord_testandfindres_inrange {
  chord_node_wire n<>;
};

struct chord_testandfindres_notinrange {
  chord_node_wire n;
  chord_node_wire succs<>;
};

union chord_testandfindres switch (chordstat status) {
 case CHORD_INRANGE:
   chord_testandfindres_inrange inrange;
 case CHORD_NOTINRANGE:
   chord_testandfindres_notinrange notinrange;
 case CHORD_STOP:
   void;
 default:
   void;
};

struct chord_gettoes_arg {
  int32_t level;
};

struct chord_findtoes_arg {
  int32_t level;
  chord_node_wire n;
};


struct chord_debruijnarg {
  chordID n;
  chordID x;
  chordID i;
  chordID k;
  unsigned upcall_prog;
  unsigned upcall_proc;
  opaque upcall_args<>;
};

struct chord_debruijnnoderes {
  chord_node_wire node;
  chord_node_wire succs<>;
  chordID i;
  chordID k;
};

union chord_debruijnres switch (chordstat status) {
 case CHORD_INRANGE:
   chord_debruijnnoderes inres;
 case CHORD_NOTINRANGE:
   chord_debruijnnoderes noderes;
 case CHORD_STOP:
   void;
 default:
   void;
};

program CHORD_PROGRAM {
	version CHORD_VERSION {
		void
		CHORDPROC_NULL (chordID) = 0;

		chord_noderes 
		CHORDPROC_GETSUCCESSOR (chordID) = 1;

		chord_noderes 
		CHORDPROC_GETPREDECESSOR (chordID) = 2;

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

		chord_nodelistres 
	        CHORDPROC_SECFINDSUCC (chord_testandfindarg) = 9;

		chord_nodeextres
		CHORDPROC_GETPRED_EXT (chordID) = 10;

		chord_nodelistextres
		CHORDPROC_GETFINGERS_EXT (chordID) = 11;

		chord_nodelistextres
		CHORDPROC_GETSUCC_EXT (chordID) = 12;

		chord_nodelistextres
          	CHORDPROC_GETTOES (chord_gettoes_arg) = 13;

		chord_debruijnres
		CHORDPROC_DEBRUIJN (chord_debruijnarg) = 14;

		chord_nodelistres
		CHORDPROC_FINDROUTE (chord_findarg) = 15;

		chord_nodelistres
		CHORDPROC_FINDTOES (chord_findtoes_arg) = 16;
	} = 1;
} = 344447;
