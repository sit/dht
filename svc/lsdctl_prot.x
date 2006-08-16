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
  int32_t coords<>;
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

struct lsdctl_rpcstat {
  str key;
  u_int64_t ncall;
  u_int64_t call_bytes;
  u_int64_t nrexmit;
  u_int64_t rexmit_bytes;
  u_int64_t nreply;
  u_int64_t reply_bytes;
  u_int64_t latency_ewma;
};

struct lsdctl_rpcstatlist {
  u_int64_t interval; /* usec since last clear */
  lsdctl_rpcstat stats<>;
};

struct lsdctl_stat {
  str desc;
  u_int64_t value;
};

struct lsdctl_blockstatus {
  chordID id;
  chord_node_wire missing<>;
};

struct lsdctl_getdhashstats_arg  {
  int vnode;
};

struct lsdctl_dhashstats {
  lsdctl_stat stats<>;
  lsdctl_blockstatus blocks<>;
};

struct lsdctl_lsdparameters {
  int nvnodes;
  str adbdsock;
  int dfrags;
  int efrags;
  int nreplica;
  net_address addr;
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

		lsdctl_rpcstatlist
		LSDCTL_GETRPCSTATS (bool) = 6;
		/** Get list and optionally clear (if true) existing stats */

		lsdctl_nodeinfolist
		LSDCTL_GETMYIDS (void) = 7;
		/** Get the list of vnode IDs on this lsd */

		lsdctl_dhashstats
		LSDCTL_GETDHASHSTATS (lsdctl_getdhashstats_arg) = 8;
		/** Tell me somthing about the status of DHash on vnode i*/
		lsdctl_lsdparameters 
		LSDCTL_GETLSDPARAMETERS (void) = 9;
		/** Return useful parameters about this lsd */
	} = 1;
} = 344500;
