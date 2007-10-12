%# include <chord_types.h>
%# include <dhash_types.h>

enum maint_status {
  MAINTPROC_OK = 0,
  MAINTPROC_ERR = 1
};

/* {{{ MAINTPROC_SETMAINT */
struct maint_setmaintarg {
  bool enable;     /* disable maintenance if false */
  bool randomize;  /* randomize start within delay interval? */
  u_int32_t delay; /* delay between rounds */
};
/* }}} */
/* {{{ MAINTPROC_INITSPACE */
struct maint_dhashinfo_t {
  chord_node host;	/* ip, port and vnum */
  dhash_ctype ctype;
  /* How to contact adbd and create an adb object */
  str dbsock;
  str dbname;
  bool hasaux;
  /* How to replicate this ctype */
  int efrags;
  int dfrags;
};
/* }}} */
/* {{{ MAINTPROC_GETREPAIRS */
struct maint_getrepairsarg {
  chord_node host;
  dhash_ctype ctype;
  int thresh;		/* no objects with more than this please */
  int count;		/* max number of repairs desired */
  chordID start;	/* where to start if not host's id */
};

struct maint_repair_t {
  /* True if the local node is responsible for this id. */
  bool responsible;
  /* Object to repair */
  chordID id;
  /* Below, port_vnnum = (port << 16) | vnnum */
  /* IP+port+vnnum of possible place to get frag/replica */
  u_int32_t src_ipv4_addr;
  u_int32_t src_port_vnnum;
  /* IP+port+vnnum to send new frag or replica to. */
  u_int32_t dst_ipv4_addr;
  u_int32_t dst_port_vnnum;
};

struct maint_getrepairsres {
  maint_status status;
  maint_repair_t repairs<>;
};
/* }}} */

program MAINT_PROGRAM {
	version MAINT_VERSION {
		void
		MAINTPROC_NULL (void) = 0;

		bool
		MAINTPROC_SETMAINT (maint_setmaintarg) = 1;

		maint_status
		MAINTPROC_INITSPACE (maint_dhashinfo_t) = 2;

		void
		MAINTPROC_LISTEN (net_address) = 3;

		maint_getrepairsres
		MAINTPROC_GETREPAIRS (maint_getrepairsarg) = 4;
	} = 1;
} = 344502;

/* vim:set foldmethod=marker: */
