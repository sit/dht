%#include <chord_prot.h>
%#include <merkle_hash.h>

enum merkle_stat {
  MERKLE_OK = 0,
  MERKLE_ERR = 1
};

/***********************************************************/
/* GETNODE */

struct getnode_arg {
  chord_node dst;
  chord_node src;
  uint32 depth;
  merkle_hash prefix;	
};

struct merkle_rpc_node {
  uint32 depth;
  merkle_hash prefix;	

  bool isleaf;
  uint64 count;
  merkle_hash hash;

  merkle_hash child_hash<64>;
};

struct getnode_res_ok {
  merkle_rpc_node node;
};

union getnode_res switch (merkle_stat status) {
 case MERKLE_OK:
   getnode_res_ok resok;
 default:
   void;
};

/*************************************************************/
/* GETBLOCKLIST */

struct getblocklist_arg {
  chord_node dst;
  chord_node src;
  merkle_hash keys<64>;
};

struct getblocklist_res_ok {
 uint32 foo;	
};

union getblocklist_res switch (merkle_stat status) {
 case MERKLE_OK:
   void;
 default:
   void;
};

/*************************************************************/
/* GETBLOCKRANGE */

struct getblockrange_arg {
  chord_node dst;
  chord_node src;
  bigint rngmin;
  bigint rngmax;
  bool bidirectional;

  merkle_hash prefix;  /* XXX kill these?? */
  int depth;           /* ??? */

  merkle_hash xkeys<64>;
};

struct getblockrange_resok {
  bool desired_xkeys<64>;
  bool will_send_blocks;
};

union getblockrange_res switch (merkle_stat status) {
 case MERKLE_OK:
   getblockrange_resok resok; 
 default:
   void;
};

program MERKLESYNC_PROGRAM {
	version MERKLESYNC_VERSION {
	        getnode_res
		MERKLESYNC_GETNODE (getnode_arg) = 2;

		getblocklist_res
                MERKLESYNC_GETBLOCKLIST (getblocklist_arg) = 3;
		 
		getblockrange_res
                MERKLESYNC_GETBLOCKRANGE (getblockrange_arg) = 4;

	} = 1;
} = 344450;
