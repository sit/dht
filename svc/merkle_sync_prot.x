%#include <chord_types.h>
%#include <dhash_types.h>
%#include <merkle_hash.h>

enum merkle_stat {
  MERKLE_OK = 0,
  MERKLE_ERR = 1
};

struct merkle_rpc_node {
  u_int32_t depth;
  merkle_hash prefix;	

  bool isleaf;
  u_int64_t count;
  merkle_hash hash;

  merkle_hash child_hash<64>;
};

struct syncdest_t {
  u_int32_t vnode;
  dhash_ctype ctype;
};

/***********************************************************/
/* GETKEYS */

struct getkeys_arg {
  u_int32_t vnode;
  dhash_ctype ctype;
  bigint rngmin;
  bigint rngmax;
};

struct getkeys_res_ok {
  bigint keys<64>;
  bool morekeys;
};

union getkeys_res switch (merkle_stat status) {
 case MERKLE_OK:
   getkeys_res_ok resok;
 default:
   void;
};


/***********************************************************/
/* SENDNODE */

struct sendnode_arg {
  u_int32_t vnode;
  dhash_ctype ctype;
  bigint rngmin;
  bigint rngmax;
  merkle_rpc_node node;
};

struct sendnode_resok {
  merkle_rpc_node node;
};

union sendnode_res switch (merkle_stat status) {
 case MERKLE_OK:
   sendnode_resok resok;
 default:
   void;
};



program MERKLESYNC_PROGRAM {
	version MERKLESYNC_VERSION {
	        sendnode_res
		MERKLESYNC_SENDNODE (sendnode_arg) = 5;

                getkeys_res
                MERKLESYNC_GETKEYS (getkeys_arg) = 6;
	} = 1;
} = 344450;
