%#include <chord_types.h>
%#if 0
%from chord_types import *
%#endif

typedef chordID cd_vnode;

/* NEWCHORD *******************************************************/

struct cd_newchord_arg {
  /* wellknownhost can be zero-length, indicating that cd should figure
   * out the local machine's address */
  chord_hostname wellknownhost;
  int wellknownport;
  /* myname can be zero-length, and the meaning is the same as for
   * wellknownhost */
  chord_hostname myname;
  int myport;
  int maxcache;
};

struct cd_newchord_res {
  chordstat stat;
};

/* UNNEWCHORD *****************************************************/

struct cd_unnewchord_res {
  chordstat stat;
};

/* NEWVNODE *******************************************************/

enum cd_routing_mode {
  MODE_SUCC,
  MODE_CHORD,
  MODE_DEBRUIJN,
  MODE_PROX,
  MODE_PROXREC,
  MODE_PNS,
  MODE_PNSREC,
  MODE_CHORDREC,
  MODE_TCPPNSREC
};

struct cd_newvnode_arg {
  cd_routing_mode routing_mode;
};

struct cd_newvnode_res_ok {
  cd_vnode vnode;
};

union cd_newvnode_res switch (chordstat stat) {
  case CHORD_OK:
    cd_newvnode_res_ok resok;
  default:
    /* CHORD_NOTINRANGE if mode is invalid */
    void;
};

/* LOOKUP *********************************************************/

struct cd_lookup_arg {
  cd_vnode vnode;
  chordID key;
};

struct cd_lookup_res_ok {
  chord_node_wire route<>;
};

union cd_lookup_res switch (chordstat stat) {
  case CHORD_OK:
    cd_lookup_res_ok resok;
  default:
    /* CHORD_NOTINRANGE if the vnode id is invalid */
    void;
};

/* program ********************************************************/

program CD_PROGRAM {
	version CD_VERSION {
		void
		CD_NULL (void) = 0;

		void
		CD_EXIT (void) = 1;
		/** Cleanly shut down cd process. */

		cd_newchord_res
		CD_NEWCHORD(cd_newchord_arg) = 2;
		/** Instantiate the chord object (returns CHORD_NOINRANGE
		    if the Chord object already exists) */

		cd_unnewchord_res
		CD_UNNEWCHORD(void) = 3;
		/** Delete the chord object (returns CHORD_NOINRANGE if the
		    Chord object doesn't exist) */

		cd_newvnode_res
		CD_NEWVNODE(cd_newvnode_arg) = 4;
		/** Instantiate a new vnode */

		cd_lookup_res
		CD_LOOKUP(cd_lookup_arg) = 5;
		/** Find successor node of chord ID */
	} = 1;
} = 344600;
