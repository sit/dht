%#include <chord_types.h>
%#include <dhc.h>

enum dhc_stat {
   DHC_OK = 0,
   DHC_CONF_MISMATCH = 1,
   DHC_LOW_PROPOSAL = 2, 
   DHC_PROP_MISMATCH = 3,
   DHC_NOT_A_REPLICA = 4
};

/* prepare message */
struct dhc_prepare_arg {
   chordID bID; 		/* ID of Public Key block */
   paxos_seqnum_t round; 	/* new Paxos number same as proposal number*/ 
   long config_seqnum; 		/* sequence number of current replica config */ 
};

struct dhc_prepare_resok {
   replica_t new_config; 	/* accepted new config for config_seqnum, if any */
				/* otherwise, set resok to NULL */
};

/* promise message */
union dhc_prepare_res switch (dhc_stat status) {	
   case DHC_OK:
	dhc_prepare_resok resok;
   default:	
	void;
};

/* accept proposal message */
struct dhc_accept_arg {
   chordID id;
   paxos_seqnum_t round;
   replica_t new_config; 	/* new configuration of nodes */
};

/* accept ack message */
union dhc_accept_res switch (dhc_stat status) {
   default:
   	void;
};

struct dhc_newconfig_arg {
   chordID bID;
   keyhash_data value;
   long old_conf_seqnum;
   replica_t new_config; 
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

union dhc_gettag_res {
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

    dhc_accept_res
    DHCPROC_ACCEPT (dhc_accept_arg) = 2;

    dhc_newconfig_res
    DHCPROC_NEWCONFIG (dhc_newconfig_arg) = 3;

    dhc_get_res
    DHCPROC_GET (dhc_get_arg) = 4;

    dhc_gettag_res
    DHCPROC_GETTAG (dhc_gettag_arg) = 5;

    void
    DHCPROC_PUT (dhc_put_arg) = 6;
     
  } = 1;	
} = 344452;