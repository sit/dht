
/*
 * This file was written by Frans Kaashoek.  Its contents is
 * uncopyrighted and in the public domain.  Of course, standards of
 * academic honesty nonetheless prevent anyone in research from
 * falsely claiming credit for this work.
 */

%#include "chord_types.h"

struct chord_findarg {
  chordID x;
  bool return_succs;
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

		chord_nodelistextres
		CHORDPROC_GETPRED_EXT (chordID) = 10;

		chord_nodelistextres
		CHORDPROC_GETSUCC_EXT (chordID) = 12;

		chord_nodelistres
        	CHORDPROC_GETPREDLIST (chordID) = 13;
		
		chord_nodelistres
		CHORDPROC_FINDROUTE (chord_findarg) = 15;
	} = 1;
} = 344447;
