%#include "chord_types.h"

struct debruijn_arg {
  chordID n;
  chordID x;
  chordID i;
  chordID k;
  unsigned upcall_prog;
  unsigned upcall_proc;
  opaque upcall_args<>;
};

struct debruijn_noderes {
  chord_node_wire node;
  chord_node_wire succs<>;
  chordID i;
  chordID k;
};

union debruijn_res switch (chordstat status) {
 case CHORD_INRANGE:
   debruijn_noderes inres;
 case CHORD_NOTINRANGE:
   debruijn_noderes noderes;
 case CHORD_STOP:
   void;
 default:
   void;
};

program DEBRUIJN_PROGRAM {
	version DEBRUIJN_VERSION {
		void
		DEBRUIJNPROC_NULL (void) = 0;

		debruijn_res
		DEBRUIJNPROC_ROUTE (debruijn_arg) = 1;
	} = 1;
} = 344453;
   
