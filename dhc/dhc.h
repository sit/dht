#ifndef _DHC_H_
#define _DHC_H_

#include <dbfe.h>
#include <arpc.h>
#include <chord.h>
#include <chord_types.h>
#include <dhash_types.h>
#include <location.h>
#include <dhc_prot.h>

// PK blocks data structure for maintaining consistency.

struct replica_t {
  u_int64_t seqnum;
  vec<ptr<location> > nodes;
  uint size;
  u_char *buf;
  
  replica_t () : seqnum (0) { };

  u_char *bytes ()
  {
    size = sizeof (u_int64_t) + nodes.size ();
    if (buf) free (buf);
    buf = (u_char *) malloc (size);
    bcopy (&seqnum, buf, sizeof (u_int64_t));
    bcopy (nodes.base (), buf + sizeof (u_int64_t), nodes.size ());
    return buf;
  }

  ~replica_t () 
  { 
    nodes.clear (); 
    if (buf) free (buf);
  }
};

#if 0
struct proposal_t {
  paxos_seqnum_t proposal_num;
  u_int64_t cur_config_seqnum;
  ptr<replica_t> new_config;
};
#endif

struct paxos_state_t {
  bool recon_inprogress;
  uint promise_recvd;
  uint accept_recvd;
  vec<chordID> acc_conf;
  uint size;
  u_char *buf;
  
  paxos_state_t () : recon_inprogress(false), promise_recvd(0), accept_recvd(0),
    buf (NULL)
  { acc_conf = vec<chordID> (); }
  
  u_char *bytes () 
  {
    if (buf) free (buf);
    size = sizeof (bool) + sizeof (uint) + sizeof (uint) + acc_conf.size ();
    buf = (u_char *) malloc (size);
    bcopy (&recon_inprogress, buf, sizeof (bool));
    bcopy (&promise_recvd, buf + sizeof (bool), sizeof (uint));
    bcopy (&accept_recvd, buf + sizeof (bool) + sizeof (uint), sizeof (uint));
    bcopy (acc_conf.base (), buf + sizeof (bool) + 2*sizeof (uint), acc_conf.size ());
    return buf;
  }
  
  ~paxos_state_t () 
  {
    if (buf) free (buf);
    acc_conf.clear ();
  }
};

struct keyhash_meta {
  ptr<replica_t> config;
  ptr<replica_t> new_config;   //next accepted config
  paxos_seqnum_t proposal;     //proposal number
  paxos_seqnum_t promised;     //promised number
  paxos_seqnum_t accepted;     //latest accepted proposal number

  ptr<paxos_state_t> pstat;

  uint size;
  u_char *buf;

  keyhash_meta () : buf (NULL)
  {
    config = New refcounted<replica_t>;
    proposal.seqnum = 0;
    bzero (&proposal.proposer, sizeof (chordID));
    promised.seqnum = 0;
    bzero (&promised.proposer, sizeof (chordID));
    accepted.seqnum = 0;
    bzero (&accepted.proposer, sizeof (chordID));    

    pstat = New refcounted<paxos_state_t>;
  }
  
  void set_new_config ()
  {
    
  }

  u_char *bytes () 
  {
    u_char *cbuf = config->bytes ();
    u_char *pbuf = pstat->bytes ();
    size = config->size + 3*sizeof (paxos_seqnum_t) + pstat->size;

    if (buf) free (buf);
    buf = (u_char *) malloc (size);
    bcopy (cbuf, buf, config->size);
    bcopy (&proposal, buf + config->size, sizeof (paxos_seqnum_t));
    bcopy (&promised, buf + config->size + sizeof (paxos_seqnum_t), 
	   sizeof (paxos_seqnum_t));
    bcopy (&accepted, buf + config->size + 2*sizeof (paxos_seqnum_t), 
	   sizeof (paxos_seqnum_t));
    bcopy (pbuf, buf + config->size + 3*sizeof (paxos_seqnum_t), 
	   pstat->size);

    return buf;
  }

  ~keyhash_meta () 
  {
    if (buf) free (buf);
  }
};

struct dhc_block {
  chordID id;
  ptr<keyhash_data> data;
  ptr<keyhash_meta> meta;
};

class dhc {
  
  ptr<vnode> myNode;
  ptr<dbfe> db;

  uint n_replica;

  void recv_prepare (user_args *);
  void recv_promise (ptr<dhc_block>, ref<dhc_prepare_res>, clnt_stat);
  void recv_propose (user_args *);
  void recv_accept (ptr<dhc_block>, ref<dhc_propose_res>, clnt_stat);
  void recv_newconfig (user_args *);
  void recv_newconfig_ack (ptr<dhc_block>, ref<dhc_newconfig_res>, clnt_stat);
  
 public:

  dhc (ptr<vnode>, str, uint, str);
  ~dhc () {};
  
  void recon (chordID);
  void dispatch (user_args *);
  
};

#endif /*_DHC_H_*/
