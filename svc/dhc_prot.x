%#include <chord_types.h>
%#include <dhc.h>

enum dhc_stat {
   DHC_OK = 0,
   DHC_CONF_MISMATCH = 1,
   DHC_LOW_PROPOSAL = 2, 
   DHC_PROP_MISMATCH = 3
};

/* prepare message */
struct dhc_prepare_arg {
   paxos_seqnum_t round; 	/* new Paxos number */ 
   chordID id; 			/* ID of Public Key block */
   long config_seqnum; 		/* configuration sequence number */ 
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
   paxos_seqnum_t round;
   chordID id;
   replica_t new_config; 	/* new configuration of nodes */
};

/* accept ack message */
union dhc_accept_res switch (dhc_stat status) {
   default:
   	void;
};

struct dhc_newconfig_arg {
   chordID id;
   long old_conf_seqnum;
   replica_t new_config; 
};

union dhc_newconfig_res switch (dhc_stat status) {
   default:
   	void;
};

program DHC_PROGRAM {
  version DHC_VERSION {

    dhc_prepare_res
    DHASHPROC_DHC_PREPARE (dhc_prepare_arg) = 1;

    dhc_accept_res
    DHASHPROC_DHC_ACCEPT (dhc_accept_arg) = 2;

    dhc_newconfig_res
    DHASHPROC_DHC_NEWCONFIG (dhc_newconfig_arg) = 3;

/* later add helper rpcs for 
   majority reads and writes
*/
     
  } = 1;	
} = 344452;