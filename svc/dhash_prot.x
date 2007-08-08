%#include <chord_types.h>
%#include <dhash_types.h>

typedef int32_t dhash_hopcount;

enum store_status {
  DHASH_STORE = 0,
  DHASH_CACHE = 1,
  DHASH_FRAGMENT = 2,
  DHASH_REPLICA = 3,
  DHASH_NOENT_NOTIFY = 4
};

struct dhash_valueattr {
  unsigned size;
  unsigned expiration; /* Seconds since epoch */
};

struct s_dhash_insertarg {
  chordID key;
  dhash_ctype ctype;
  dhash_value data;
  int offset;
  store_status type;
  dhash_valueattr attr;
  int32_t nonce; /* used only if this is a fetch complete */
};

struct s_dhash_fetch_arg {
  chordID key;
  dhash_ctype ctype;
  int32_t nonce;
};

struct dhash_pred {
  chord_node_wire p;
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

struct dhash_fetchcomplete_res {
  dhash_value res;
  int32_t offset;
  dhash_valueattr attr;
};

union dhash_fetchiter_res switch (dhash_stat status) {
 case DHASH_INPROGRESS:
   void;
 case DHASH_CONTINUE:
   void;
 case DHASH_NOENT:
   void;
 default:
   void;
};


struct s_dhash_block_arg {
  dhash_value res;
  int32_t offset;
  dhash_valueattr attr;
  chordID source;
  /* if a the sender of this RPC doesn't have the block 
   * then he sends back a list of successors.
   */
  chord_node_wire nodelist<>; 
};


struct dhash_fetchrec_arg {
  chordID key;			/* key to route towards */
  dhash_ctype ctype;
  
  chord_node_wire path<>;	/* path accumulator */
};

struct dhash_fetchrec_resok {
  dhash_value res;
  u_int32_t times<>;
  chord_node_wire path<>;
};

struct dhash_fetchrec_resdefault {
  chord_node_wire path<>;
  u_int32_t times<>;
};

union dhash_fetchrec_res switch (dhash_stat status) {
 case DHASH_OK:
   dhash_fetchrec_resok resok;
 default:
   dhash_fetchrec_resdefault resdef;
};

program DHASH_PROGRAM {
  version DHASH_VERSION {
    void
    DHASHPROC_NULLX (void) = 0;

    /* Basic store */
    dhash_storeres
    DHASHPROC_STORE (s_dhash_insertarg) = 1;

    /* Ideal for TCP transfers, minimizing new connections */
    dhash_fetchrec_res
    DHASHPROC_FETCHREC (dhash_fetchrec_arg) = 2;

    /* Downloads over UDP */
    dhash_fetchiter_res
    DHASHPROC_FETCHITER (s_dhash_fetch_arg) = 3;

    /* RPC back to fetch initiator with data */
    void
    DHASHPROC_FETCHCOMPLETE (s_dhash_insertarg) = 5;
  } = 1;
} = 344449;
