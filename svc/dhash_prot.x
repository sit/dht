%#include <chord_prot.h>

typedef opaque dhash_value<>;

enum dhash_stat {
  DHASH_OK = 0,
  DHASH_NOENT = 1
};

struct dhash_insertarg {
  sfs_ID key;
  dhash_value data;
};

struct dhash_resok {
  dhash_value res;
};

union dhash_res switch (dhash_stat status) {
 case DHASH_OK:
   dhash_resok resok;
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

    dhash_stat
    DHASHPROC_STORE (dhash_insertarg) = 1;
    
    dhash_res
    DHASHPROC_FETCH (sfs_ID) = 2;

  } = 1;
} = 344449;

