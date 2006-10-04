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
  bigint keys<>;
  bool morekeys;
};

union getkeys_sample_res switch (sample_stat status) {
 case SAMPLE_OK:
   getkeys_sample_res_ok resok;
 default:
   void;
};


/***********************************************************/
/* GETDATA */

struct getdata_sample_arg {
  dhash_ctype ctype;
  u_int32_t vnode;
  bigint keys<>;
};

struct getdata_sample_resok {
  dhash_value data<>;
};

union getdata_sample_res switch (sample_stat status) {
 case SAMPLE_OK:
   getdata_sample_resok resok;
 default:
   void;
};



program SAMPLE_PROGRAM {
	version SAMPLE_VERSION {
	        getdata_sample_res
		SAMPLE_GETDATA (getdata_sample_arg) = 0;

                getkeys_sample_res
                SAMPLE_GETKEYS (getkeys_sample_arg) = 1;
	} = 1;
} = 344455;
