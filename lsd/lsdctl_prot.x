%#include <chord_types.h>

struct lsdctl_setreplicate_arg {
  bool enable;    /* disable replication if false */
  bool randomize; /* start at 0 at the same time if false */
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
	} = 1;
} = 344500;
