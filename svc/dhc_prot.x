%#include <chord_types.h>
%#include <dhash_types.h>

enum dhc_stat {
   DHC_OK = 0,
   DHC_CONF_MISMATCH = 1,
   DHC_LOW_PROPOSAL = 2, 
   DHC_PROP_MISMATCH = 3,
   DHC_NOT_A_REPLICA = 4,
   DHC_CHORDERR = 5,
   DHC_RECON_INPROG = 6,
   DHC_OLD_VER = 7,
   DHC_BLOCK_NEXIST = 8
};

struct paxos_seqnum_t {
  u_int64_t seqnum; 		
  chordID proposer;
};

/* prepare message */
struct dhc_prepare_arg {
   chordID bID; 		/* ID of Public Key block */
   paxos_seqnum_t round; 	/* new Paxos number same as proposal number*/ 
   u_int64_t config_seqnum; 	/* sequence number of current replica config */ 
};

struct dhc_prepare_resok {
   chordID new_config<>;	/* accepted new config for config_seqnum, if any */
				/* otherwise, set resok to NULL */
};

/* promise message */
union dhc_prepare_res switch (dhc_stat status) {	
   case DHC_OK:
	dhc_prepare_resok resok;
   default:	
	void;
};

/* proposal message, previously called accept message */
struct dhc_propose_arg {
   chordID bID;
   paxos_seqnum_t round;
   chordID new_config<>;
};

/* accept message, previously called accept ack */
union dhc_propose_res switch (dhc_stat status) {
   default:
   	void;
};

struct tag_t {
  u_int64_t ver;
  chordID writer;
};

struct keyhash_data {
  tag_t tag;
  dhash_value data;
};

struct dhc_newconfig_arg {
   chordID bID;
   keyhash_data data;
   u_int64_t old_conf_seqnum;
   chordID new_config<>;
};

union dhc_newconfig_res switch (dhc_stat status) {
   default:
   	void;
};

struct dhc_get_arg {
   chordID bID;
};

struct dhc_get_resok {
   keyhash_data data;
}; 

union dhc_get_res switch (dhc_stat status) {
   case DHC_OK:
	dhc_get_resok resok;
   default:
	void;
};

struct dhc_gettag_arg {
   chordID bID;
   chordID writer;
};

struct dhc_gettag_resok {
   chordID primary;
   tag_t new_tag;
};

union dhc_gettag_res switch (dhc_stat status) {
   case DHC_OK:
	dhc_gettag_resok resok;
   default:
	void;
};

struct dhc_put_arg {
   chordID bID;
   keyhash_data data;
};

program DHC_PROGRAM {
  version DHC_VERSION {

    dhc_prepare_res
    DHCPROC_PREPARE (dhc_prepare_arg) = 1;

    dhc_propose_res
    DHCPROC_PROPOSE (dhc_propose_arg) = 2;

    dhc_newconfig_res
    DHCPROC_NEWCONFIG (dhc_newconfig_arg) = 3;

    dhc_get_res
    DHCPROC_GET (dhc_get_arg) = 4;

    dhc_get_res
    DHCPROC_GETBLOCK (dhc_get_arg) = 5;

    dhc_gettag_res
    DHCPROC_GETTAG (dhc_gettag_arg) = 6;

    void
    DHCPROC_PUT (dhc_put_arg) = 7;
     
  } = 1;	
} = 344452;
