%#include <chord_prot.h>

typedef opaque dhash_value<>;

enum dhash_stat {
  DHASH_OK = 0,
  DHASH_NOENT = 1,
  DHASH_NOTPRESENT = 2,
  DHASH_RETRY = 3,
  DHASH_STORED = 4,
  DHASH_CACHED = 5,
  DHASH_REPLICATED = 6
};

enum store_status {
  DHASH_STORE = 0,
  DHASH_CACHE = 1,
  DHASH_REPLICA = 3
};

struct dhash_insertarg {
  sfs_ID key;
  dhash_value data;
  store_status type;
};

struct dhash_resok {
  dhash_value res;
};

struct dhash_pred {
  sfs_ID n;
};

union dhash_res switch (dhash_stat status) {
 case DHASH_OK:
   dhash_resok resok;
 case DHASH_RETRY:
   dhash_pred pred;
 default:
   void;
};

union dhash_storeres switch (dhash_stat status) {
 case DHASH_RETRY:
   dhash_pred pred;
 default:
   void;
};

program DHASHCLNT_PROGRAM {
	version DHASHCLNT_VERSION {

		void 
		DHASHPROC_NULL (void) = 0;

		dhash_res
		DHASHPROC_LOOKUP (sfs_ID) = 1;

		dhash_stat
        	DHASHPROC_INSERT (dhash_insertarg) = 2;

	} = 1;
} = 344448;

program DHASH_PROGRAM {
  version DHASH_VERSION {

    dhash_storeres
    DHASHPROC_STORE (dhash_insertarg) = 1;
    
    dhash_res
    DHASHPROC_FETCH (sfs_ID) = 2;

    dhash_stat
    DHASHPROC_CHECK (sfs_ID) = 3;

  } = 1;
} = 344449;

