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
   DHC_BLOCK_NEXIST = 8,
   DHC_W_INPROG = 9,
   DHC_NOT_PRIMARY = 10,
   DHC_BLOCK_EXIST = 11,
   DHC_DHASHERR = 12,
   DHC_NOT_MASTER = 13
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

/* promise message and reply for ask */
union dhc_prepare_res switch (dhc_stat status) {	
   case DHC_OK:
	dhc_prepare_resok resok;
   case DHC_LOW_PROPOSAL:
	paxos_seqnum_t promised;
   default:	
	void;
};

/* proposal message, previously called accept message */
struct dhc_propose_arg {
   chordID bID;
   paxos_seqnum_t round;
   u_int64_t config_seqnum;
   chordID new_config<>;
};

struct tag_t {
  u_int64_t ver;
  chordID writer;
};

struct keyhash_data {
  tag_t tag;
  dhash_value data;
};

/* accept message, previously called accept ack */
union dhc_propose_res switch (dhc_stat status) {
   case DHC_OK:
     keyhash_data data;
   default:
     void;
};

struct dhc_newconfig_arg {
   chordID bID;
   chordID mID;
   keyhash_data data;
   u_int64_t old_conf_seqnum;
   chordID new_config<>;
};

struct dhc_newconfig_res {
   dhc_stat status;
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

struct dhc_put_arg {
   chordID bID;
   chordID writer;
   dhash_value value;
};

struct dhc_putblock_arg {
   chordID bID;
   keyhash_data new_data;
};

struct dhc_put_res {
   dhc_stat status;
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

    dhc_put_res
    DHCPROC_PUT (dhc_put_arg) = 6;

    dhc_put_res
    DHCPROC_PUTBLOCK (dhc_putblock_arg) = 7;
    
    dhc_put_res
    DHCPROC_NEWBLOCK (dhc_put_arg) = 8; 

    dhc_prepare_res 
    DHCPROC_ASK (dhc_propose_arg) = 9;

    dhc_prepare_res
    DHCPROC_CMP (dhc_prepare_arg) = 10;

  } = 1;	
} = 344452;
