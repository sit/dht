%#include <chord_types.h>
%#include <dhash_types.h>

%#if 0
%# Imports for Python
%from chord_types import *
%from dhash_types import *
%#endif /* 0 */

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
  dhash_value block;      /* the data block */
  u_int32_t expiration;	  /* Seconds since epoch */
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
  u_int32_t expiration;	  /* Seconds since epoch */
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
		DHASHPROC_NULL (void) = 0;

	        dhash_insert_res
		DHASHPROC_INSERT (dhash_insert_arg) = 1;

                dhash_retrieve_res
		DHASHPROC_RETRIEVE (dhash_retrieve_arg) = 2;
	} = 1;
} = 344448;
