%#include <chord_prot.h>

typedef opaque dhash_value<64>;

program DHASH_PROGRAM {
	version DHASH_VERSION {

		void 
		DHASHPROC_NULL (void) = 0;

		dhash_value
		DHASHPROC_LOOKUP (sfs_ID) = 1;

	} = 1;
} = 344448;
