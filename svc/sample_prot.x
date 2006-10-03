%#include <chord_types.h>
%#include <dhash_types.h>

enum sample_stat {
  SAMPLE_OK = 0,
  SAMPLE_ERR = 1
};

/***********************************************************/
/* GETKEYS */

struct getkeys_sample_arg {
  dhash_ctype ctype;
  u_int32_t vnode;
  bigint rngmin;
  bigint rngmax;
};

struct getkeys_sample_res_ok {
  bigint keys<64>;
  bool morekeys;
};

union getkeys_sample_res switch (sample_stat status) {
 case SAMPLE_OK:
   getkeys_sample_res_ok resok;
 default:
   void;
};


/***********************************************************/
/* SENDNODE */

struct sendnode_sample_arg {
  dhash_ctype ctype;
  u_int32_t vnode;
  bigint rngmin;
  bigint rngmax;
};

struct sendnode_sample_resok {
  u_int32_t xxx;
};

union sendnode_sample_res switch (sample_stat status) {
 case SAMPLE_OK:
   sendnode_sample_resok resok;
 default:
   void;
};



program SAMPLE_PROGRAM {
	version SAMPLE_VERSION {
	        sendnode_sample_res
		SAMPLE_SENDNODE (sendnode_sample_arg) = 0;

                getkeys_sample_res
                SAMPLE_GETKEYS (getkeys_sample_arg) = 1;
	} = 1;
} = 344455;
