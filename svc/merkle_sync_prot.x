%#include <chord_types.h>
%#include <merkle_hash.h>

enum merkle_stat {
  MERKLE_OK = 0,
  MERKLE_ERR = 1
};

struct merkle_rpc_node {
  uint32 depth;
  merkle_hash prefix;	

  bool isleaf;
  uint64 count;
  merkle_hash hash;

  merkle_hash child_hash<64>;
};


/***********************************************************/
/* GETKEYS */

struct getkeys_arg {
  bigint rngmin;
  bigint rngmax;
};

struct getkeys_res_ok {
  merkle_hash keys<64>;
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
