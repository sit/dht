%#include <chord_prot.h>

typedef opaque dhash_value<>;
typedef int32 dhash_hopcount;

enum dhash_stat {
  DHASH_OK = 0,
  DHASH_ERR = 7,
  DHASH_CHORDERR = 8,
  DHASH_RPCERR = 9,
  DHASH_STOREERR = 10,
  DHASH_NOENT = 1,
  DHASH_NOTPRESENT = 2,
  DHASH_RETRY = 3,
  DHASH_STORED = 4,
  DHASH_CACHED = 5,
  DHASH_REPLICATED = 6,
  DHASH_STORE_PARTIAL = 11,
  DHASH_COMPLETE = 12,
  DHASH_CONTINUE = 13,
  DHASH_INVALIDVNODE = 14,
  DHASH_RFETCHDONE = 15,
  DHASH_RFETCHFORWARDED = 16,
  DHASH_STORE_NOVERIFY = 17,
  DHASH_WAIT = 18
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

struct dhash_valueattr {
  unsigned size;
};


struct s_dhash_insertarg {
  chordID v;
  chordID key;
  dhash_value data;
  int offset;
  store_status type;
  dhash_valueattr attr;
};

struct s_dhash_fetch_arg {
  chordID v;
  chordID key;
  chord_node from;
  int32 start;
  int32 len;
  int32 cookie;
  int32 nonce;
  bool lease;
};

struct s_dhash_keystatus_arg {
  chordID v;
  chordID key;
};


struct dhash_pred {
  chord_node p;
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
  chordID v;
  chordID pred_id;
  chordID start;
};


struct dhash_storeresok {
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
  int32 offset;
  dhash_valueattr attr;
  chordID source;
  int32 cookie;
  int32 lease;
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
  chordID v;
  int32 nonce;
  dhash_value res;
  int32 offset;
  dhash_valueattr attr;
  chordID source;
  int32 cookie;
  int32 lease;
};

program DHASH_PROGRAM {
  version DHASH_VERSION {

    dhash_storeres
    DHASHPROC_STORE (s_dhash_insertarg) = 1;

    dhash_getkeys_res
    DHASHPROC_GETKEYS (s_dhash_getkeys_arg) = 3;

    dhash_fetchiter_res
    DHASHPROC_FETCHITER (s_dhash_fetch_arg) = 4;

    dhash_stat
    DHASHPROC_BLOCK (s_dhash_block_arg) = 5;
  } = 1;
} = 344449;



//  --------------------------------------------------------------------------
//  The DHASHGATEWAY_PROGRAM is the very narrow interface between clients of 
//  dhash (such as chordcd, sfsrodb, dbm) and the lsd process.  Since this RPC 
//  interface is intended to be transported over a unix domain socket, blocks 
//  are retrieved/inserted in their entirety, ie. are not broken into chunks. 


struct dhash_insert_arg {
  chordID     blockID;    // the key
  dhash_value block;  // the data block
  dhash_ctype ctype;  // and type of the data block
  bool usecachedsucc;
};

struct dhash_retrieve_arg {
  chordID blockID;
  bool usecachedsucc;
  bool askforlease;
};

struct dhash_retrieve_resok {
  dhash_value block;
  int hops;
  int errors;
  int lease;
};

struct dhash_insert_resok {
  chordID destID;
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
		DHASHPROC_RETRIEVE (dhash_retrieve_arg) = 3;
                
		dhash_stat
         	DHASHPROC_ACTIVE (int32) = 4;
		
	} = 1;
} = 344448;





