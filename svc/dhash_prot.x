%#include <chord_types.h>
%#include <dhash_types.h>

typedef int32_t dhash_hopcount;

enum store_status {
  DHASH_STORE = 0,
  DHASH_CACHE = 1,
  DHASH_FRAGMENT = 2,
  DHASH_REPLICA = 3
};

enum dhash_dbtype {
  DHASH_BLOCK = 0,
  DHASH_FRAG = 1
};

struct dhash_valueattr {
  unsigned size;
};

enum dhash_offer_status {
  DHASH_HOLD = 1,
  DHASH_DELETE = 2,
  DHASH_SENDTO = 3
};

struct s_dhash_insertarg {
  chordID key;
  dhash_ctype ctype;
  dhash_dbtype dbtype;
  dhash_value data;
  int offset;
  store_status type;
  dhash_valueattr attr;
  bool last; /* used by the merkle code only */
};

struct s_dhash_fetch_arg {
  chordID key;
  dhash_ctype ctype;
  dhash_dbtype dbtype;
  int32_t start;
  int32_t len;
  int32_t cookie;
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


struct dhash_offer_arg {
  chordID keys<64>;
};

struct dhash_offer_resok {
  chord_node_wire dest<64>;
  dhash_offer_status accepted<64>;
};

union dhash_offer_res switch (dhash_stat status) {
 case DHASH_OK:
   dhash_offer_resok resok;
default:
   void; 
};

struct dhash_fetchrec_arg {
  chordID key;			/* key to route towards */
  dhash_ctype ctype;
  dhash_dbtype dbtype;
  
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

enum bsm_transition {
   BSM_MISSING,
   BSM_FOUND
};

struct dhash_bsmupdate_arg {
  bsm_transition t;
  /* If we're missing locally, n is the one who told us */
  /* Else, n is where it is missing */
  chord_node_wire n;
  bool local;

  chordID key;
  /* Unused? */
  dhash_ctype ctype;
  dhash_dbtype dbtype;
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

    /* "Put that block back where it came from, or so help me..." */
    dhash_offer_res
    DHASHPROC_OFFER (dhash_offer_arg) = 4;

    /* For the syncer to update DHash's bsm.
     * Should only be called from localhost.
     */
    void
    DHASHPROC_BSMUPDATE (dhash_bsmupdate_arg) = 5;
  } = 1;
} = 344449;
