%#include <chord_types.h>

/** Outgoing recursive RPCs */
enum recroute_route_stat {
  RECROUTE_ACCEPTED = 0,
  RECROUTE_REJECTED = 1
};

struct recroute_route_arg {
  unsigned routeid;		/* client internal identifier */
  chord_node_wire origin;	/* where to send responses */
  
  chordID x;			/* key to route towards */
  unsigned succs_desired;	/* how many successors of key desired */

  /* upcall crap */
  unsigned upcall_prog;
  unsigned upcall_proc;
  opaque upcall_args<>;
  
  unsigned retries;		/* number of times node's first try failed */
  chord_node_wire path<>;	/* path accumulator */
};

/** Almost there; overlap case. Looks a lot like above... */
struct recroute_penult_arg {
  unsigned routeid;
  chord_node_wire origin;	/* where to send responses */
  
  chordID x;			/* key to route towards */
  unsigned succs_desired;	/* how many successors of key desired */
  chord_node_wire successors <>; /* Partial successor list */

  /* upcall crap */
  unsigned upcall_prog;
  unsigned upcall_proc;
  opaque upcall_args<>;
  
  unsigned retries;		/* number of times node's first try failed */
  chord_node_wire path<>;	/* path accumulator */  
};


/** Incoming RPC completions */
enum recroute_complete_stat {
  RECROUTE_ROUTE_OK = 0,
  RECROUTE_ROUTE_FAILED = 1	/* Perhaps no available next hops */
};

struct recroute_complete_ok {
  chord_node_wire successors <>; /* First entry should be key's true successor */
};

struct recroute_complete_failure {
  chord_node_wire failed_hop;	/* node we were trying to hit next */
  unsigned failed_stat;		/* RPC level failure? */
};

union recroute_complete_res switch (recroute_complete_stat status) {
 case RECROUTE_ROUTE_OK:
   recroute_complete_ok robody;
 case RECROUTE_ROUTE_FAILED:
   recroute_complete_failure rfbody;
};

struct recroute_complete_arg {
  unsigned routeid;		/* to associate with request */
  chord_node_wire path <>;      /* partial or complete path, depend on status */
  unsigned retries;		/* number of times node's first try failed */

  recroute_complete_res body;
};

program RECROUTE_PROGRAM {
  version RECROUTE_VERSION {
    void
    RECROUTEPROC_NULL (void) = 0;

    recroute_route_stat
    RECROUTEPROC_ROUTE (recroute_route_arg) = 1;

    void
    RECROUTEPROC_COMPLETE (recroute_complete_arg) = 2;

    void
    RECROUTEPROC_PENULTIMATE (recroute_penult_arg) = 3;
  } = 1;
} = 344460;
