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
  DHASH_STORE_PARTIAL = 11
};

enum store_status {
  DHASH_STORE = 0,
  DHASH_CACHE = 1,
  DHASH_REPLICA = 3
};

struct dhash_blockattr {
  unsigned size;
};

struct dhash_insertarg {
  chordID key;
  dhash_value data;
  int offset;
  store_status type;
  dhash_blockattr attr;
};

struct dhash_fetch_arg {
  chordID key;
  int32 start;
  int32 len;
};

struct dhash_pred {
  chordID n;
};

struct dhash_resok {
  dhash_value res;
  int32 offset;
  dhash_blockattr attr;
  int32 hops;
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
struct dhash_getkeys_arg {
  chordID pred_id;
};

union dhash_res switch (dhash_stat status) {
 case DHASH_OK:
   dhash_resok resok;
 case DHASH_RETRY:
   dhash_pred pred;
default:
   void;
};

struct dhash_storeresok {
  bool done;
};

union dhash_storeres switch (dhash_stat status) {
 case DHASH_RETRY:
   dhash_pred pred;
 default:
   dhash_storeresok resok;
};

program DHASHCLNT_PROGRAM {
	version DHASHCLNT_VERSION {

		void 
		DHASHPROC_NULL (void) = 1;

		dhash_res
		DHASHPROC_LOOKUP (dhash_fetch_arg) = 2;

		dhash_storeres
        	DHASHPROC_INSERT (dhash_insertarg) = 3;

	} = 1;
} = 344448;

program DHASH_PROGRAM {
  version DHASH_VERSION {

    dhash_storeres
    DHASHPROC_STORE (dhash_insertarg) = 1;

    dhash_res
    DHASHPROC_FETCH (dhash_fetch_arg) = 2;

    dhash_getkeys_res
    DHASHPROC_GETKEYS (dhash_getkeys_arg) = 3;

    dhash_stat
    DHASHPROC_KEYSTATUS (chordID) = 4;

  } = 1;
} = 344449;


