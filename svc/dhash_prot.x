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
  DHASH_STORE_NOVERIFY = 17
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

struct dhash_blockattr {
  unsigned size;
  dhash_ctype ctype;
};

struct dhash_insertarg {
  chordID key;
  dhash_value data;
  int offset;
  store_status type;
  dhash_blockattr attr;
};


struct s_dhash_insertarg {
  chord_vnode v;
  chordID key;
  dhash_value data;
  int offset;
  store_status type;
  dhash_blockattr attr;
};

struct s_dhash_fetch_arg {
  chord_vnode v;
  chordID key;
  int32 start;
  int32 len;
};

struct dhash_fetch_arg {
  chordID key;
  int32 start;
  int32 len;
};

struct s_dhash_keystatus_arg {
  chord_vnode v;
  chordID key;
};

struct dhash_transfer_arg {
  dhash_fetch_arg farg;
  chordID source;
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
  chord_vnode v;
  chordID pred_id;
};

struct dhash_resok {
  dhash_value res;
  int32 offset;
  dhash_blockattr attr;
  int32 hops;
  chordID source;
};

union dhash_res switch (dhash_stat status) {
 case DHASH_OK:
   dhash_resok resok;
 default:
   int32 hops;
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

struct dhash_fetchiter_continue_res {
  chord_node next;
  /////chord_node succ_list<>;
};

struct dhash_fetchiter_complete_res {
  dhash_value res;
  int32 offset;
  dhash_blockattr attr;
  int32 hops;
  chordID source;
};

union dhash_fetchiter_res switch (dhash_stat status) {
 case DHASH_COMPLETE:
   dhash_fetchiter_complete_res compl_res;
 case DHASH_CONTINUE:
   dhash_fetchiter_continue_res cont_res;
 default:
   void;
};

struct dhash_send_arg {
  dhash_insertarg iarg;
  chordID dest;
};

program DHASHGATEWAY_PROGRAM {
	version DHASHCLNT_VERSION {

		void 
		DHASHPROC_NULL (void) = 1;

		dhash_res
		DHASHPROC_LOOKUP (dhash_fetch_arg) = 2;

		dhash_res
		DHASHPROC_TRANSFER (dhash_transfer_arg) = 3;

		dhash_storeres
        	DHASHPROC_INSERT (dhash_insertarg) = 4;

		dhash_storeres
		DHASHPROC_SEND (dhash_send_arg) = 5;

		dhash_stat
         	DHASHPROC_ACTIVE (int32) = 6;
		
	} = 1;
} = 344448;

program DHASH_PROGRAM {
  version DHASH_VERSION {

    dhash_storeres
    DHASHPROC_STORE (s_dhash_insertarg) = 1;

    dhash_getkeys_res
    DHASHPROC_GETKEYS (s_dhash_getkeys_arg) = 3;

    dhash_stat
    DHASHPROC_KEYSTATUS (s_dhash_keystatus_arg) = 4;

    dhash_fetchiter_res
    DHASHPROC_FETCHITER (s_dhash_fetch_arg) = 5;

  } = 1;
} = 344449;



