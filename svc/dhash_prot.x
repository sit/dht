%#include <chord_types.h>
%#include <merkle_hash.h>

typedef opaque dhash_value<>;
typedef int32_t dhash_hopcount;

enum dhash_stat {
  DHASH_OK = 0,
  DHASH_NOENT = 1,
  DHASH_NOTPRESENT = 2,
  DHASH_RETRY = 3,
  DHASH_STORED = 4,
  RPC_NOHANDLER = 5,
  DHASH_REPLICATED = 6,
  DHASH_ERR = 7,
  DHASH_CHORDERR = 8,
  DHASH_RPCERR = 9,
  DHASH_STOREERR = 10,
  DHASH_STORE_PARTIAL = 11,
  DHASH_COMPLETE = 12,
  DHASH_CONTINUE = 13,
  DHASH_INVALIDVNODE = 14,
  DHASH_RFETCHDONE = 15,
  DHASH_RFETCHFORWARDED = 16,
  DHASH_STORE_NOVERIFY = 17,
  DHASH_STALE = 18,
  DHASH_CACHED = 19,
  DHASH_WAIT = 20,
  DHASH_TIMEDOUT = 21
};

enum store_status {
  DHASH_STORE = 0,
  DHASH_CACHE = 1,
  DHASH_REPLICA = 3
};

enum dhash_ctype {
  DHASH_CONTENTHASH = 0,
  DHASH_KEYHASH = 1,
  DHASH_DNSSEC = 2,
  DHASH_NOAUTH = 3,
  DHASH_APPEND = 4,
  DHASH_UNKNOWN = 5
};

enum dhash_dbtype {
  DHASH_BLOCK = 0,
  DHASH_FRAG = 1
};

struct dhash_valueattr {
  unsigned size;
};


struct s_dhash_insertarg {
  chordID key;
  dhash_ctype ctype;
  dhash_dbtype dbtype;
  chord_node_wire from;
  dhash_value data;
  int offset;
  int32_t nonce; /* XXX remove */
  store_status type;
  dhash_valueattr attr;
  bool last; /* used by the merkle code only */
};

struct s_dhash_fetch_arg {
  chordID key;
  dhash_ctype ctype;
  dhash_dbtype dbtype;
  chord_node_wire from;
  int32_t start;
  int32_t len;
  int32_t cookie;
  int32_t nonce; /* XXX remove */
};

struct s_dhash_keystatus_arg {
  chordID key;
};


struct dhash_pred {
  chord_node_wire p;
};

struct dhash_getkeys_ok {
  chordID keys<>;
};

union dhash_getkeys_res switch (dhash_stat status) {
 case DHASH_OK:
   dhash_getkeys_ok resok;
default:
   void; 
};

struct s_dhash_getkeys_arg {
  chordID pred_id;
  chordID start;
};


struct dhash_storeresok {
  bool already_present;
  bool done;
  chordID source;
};

union dhash_storeres switch (dhash_stat status) {
 case DHASH_RETRY:
   dhash_pred pred;
 case DHASH_OK:
   dhash_storeresok resok;
 default:
   void;
};

struct dhash_fetchiter_complete_res {
  dhash_value res;
  int32_t offset;
  dhash_valueattr attr;
  chordID source;
  int32_t cookie;
};

union dhash_fetchiter_res switch (dhash_stat status) {
 case DHASH_COMPLETE:
   dhash_fetchiter_complete_res compl_res;
 case DHASH_CONTINUE:
   void;
 case DHASH_NOENT:
   void;
 default:
   void;
};


struct s_dhash_block_arg {
  int32_t nonce; /* XXX remove */
  dhash_value res;
  int32_t offset;
  dhash_valueattr attr;
  chordID source;
  int32_t cookie;
  /* if a the sender of this RPC doesn't have the block 
   * then he sends back a list of successors.
   */
  chord_node_wire nodelist<>; 
};


struct s_dhash_storecb_arg {
  int32_t nonce; /* XXX remove */
  dhash_stat status;
};


struct dhash_offer_arg {
  bigint keys<64>;
};

struct dhash_offer_resok {
   bool accepted<64>;
};

union dhash_offer_res switch (dhash_stat status) {
 case DHASH_OK:
   dhash_offer_resok resok;
default:
   void; 
};


program DHASH_PROGRAM {
  version DHASH_VERSION {

    dhash_storeres
    DHASHPROC_STORE (s_dhash_insertarg) = 1;

/* not used
    dhash_stat
    DHASHPROC_STORECB (s_dhash_storecb_arg) = 2;
*/

    dhash_getkeys_res
    DHASHPROC_GETKEYS (s_dhash_getkeys_arg) = 3;

    dhash_fetchiter_res
    DHASHPROC_FETCHITER (s_dhash_fetch_arg) = 4;

/* not used
    dhash_stat
    DHASHPROC_BLOCK (s_dhash_block_arg) = 5;
*/

    dhash_offer_res
    DHASHPROC_OFFER (dhash_offer_arg) = 6;

  } = 1;
} = 344449;



/*  -------------------------------------------------------------------------- 
 *  The DHASHGATEWAY_PROGRAM is the very narrow interface between clients of 
 *  dhash (such as chordcd, sfsrodb, dbm) and the lsd process.  Since this RPC 
 *  interface is intended to be transported over a unix domain socket, blocks 
 *  are retrieved/inserted in their entirety, ie. are not broken into chunks. 
 */

/* struct dhash_contenthash_block {
   chordID     key;    
   dhash_value block;  
   };
 
   struct dhash_publickey_block {
   sfs_pubkey2 pub_key;
   sfs_sig2 sig;
   dhash_value block;
   };
 
   union dhash_rpc_block switch (dhash_ctype ctype) {
   case DHASH_CONTENTHASH:
   dhash_contenthash_block chash;
   case DHASH_KEYHASH:
   dhash_publickey_block pkhash;
   default:
   void;
   };
 
   struct dhash_insert2_arg {
   dhash_rpc_block block;
   int options;
   };
*/

/* if we get many more optional arguments consider switching to union */
struct dhash_insert_arg {
  chordID   blockID;      /* the key */
  dhash_ctype ctype;
  int32_t len; /* XXX fix to not need len */
  dhash_value block;      /* the data block */
  int options;
  chordID guess; /* a guess as to where this block will end up */
};

struct dhash_retrieve_arg {
  chordID blockID;
  dhash_ctype ctype; /* XXX fix to not need */
  int options;
  chordID guess;  /* a guess as to the location of the block */
};

struct dhash_retrieve_resok {
  dhash_value block;
  dhash_ctype ctype; /* XXX fix to not need */
  int32_t len; /* XXX not needed */
  int hops;
  int errors;
  int retries;
  u_int32_t times<>;
  chordID path<>;
};

struct dhash_insert_resok {
  chordID path<>;
};

union dhash_insert_res switch (dhash_stat status) {
 case DHASH_OK:
   dhash_insert_resok resok;
 default:
   void;
};

union dhash_retrieve_res switch (dhash_stat status) {
 case DHASH_OK:
   dhash_retrieve_resok resok;
 default:
   void;
};


program DHASHGATEWAY_PROGRAM {
	version DHASHCLNT_VERSION {
		void 
		DHASHPROC_NULL (void) = 1;

	        dhash_insert_res
		DHASHPROC_INSERT (dhash_insert_arg) = 2;

                dhash_retrieve_res
		DHASHPROC_RETRIEVE (dhash_retrieve_arg) = 4;
                
		dhash_stat
         	DHASHPROC_ACTIVE (int32_t) = 5;

	} = 1;
} = 344448;





