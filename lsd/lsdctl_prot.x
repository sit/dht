%#include <chord_types.h>

struct lsdctl_setreplicate_arg {
  bool enable;    /* disable replication if false */
  bool randomize; /* start at 0 at the same time if false */
};

/* Cf location.h */
struct lsdctl_nodeinfo {
  chordID n;
  net_address addr;
  int32_t vnode_num;
  int32_t coords[3]; /* XXX hardcoded length of 3; cf NCOORD in chord.h */
  u_int32_t a_lat;
  u_int32_t a_var;
  u_int32_t nrpc;
  bool pinned;
  bool alive;
  int32_t dead_time;
};

struct lsdctl_nodeinfolist {
  lsdctl_nodeinfo nlist<>;
};

program LSDCTL_PROG {
	version LSDCTL_VERSION {
		void
		LSDCTL_NULL (void) = 0;

		void
		LSDCTL_EXIT (void) = 1;
		/** Cleanly shut down lsd process. */

		void
		LSDCTL_SETTRACELEVEL (int) = 2;
		/** Set the trace level for logging */
		
		bool
		LSDCTL_SETSTABILIZE (bool) = 3;
		/** Enable (true) or disable (false) stabilization.
		 *  Returns new stabilization state. */
		 
		bool
		LSDCTL_SETREPLICATE (lsdctl_setreplicate_arg) = 4;
		/** Enable (true) or disable (false) replica maintenance.
		 *  Returns new replication state. */

		lsdctl_nodeinfolist
		LSDCTL_GETLOCTABLE (int) = 5;
		/** Return location table for given vnode.
		 *  This is often the same, regardless of vnode. */
	} = 1;
} = 344500;
