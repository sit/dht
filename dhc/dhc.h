#ifndef _DHC_H_
#define _DHC_H_

#include <dbfe.h>
#include <arpc.h>
#include <chord.h>
#include <chord_types.h>
#include <dhash_types.h>
#include <location.h>
#include <locationtable.h>
#include <dhc_prot.h>

extern void set_locations (vec<ptr<location> >, ptr<vnode>, vec<chordID>);

// PK blocks data structure for maintaining consistency.

struct replica_t {
  u_int64_t seqnum;
  vec<chordID> nodes;
  
  replica_t () : seqnum (0) { };

  ~replica_t () { nodes.clear (); }
};

struct keyhash_meta {
  replica_t config;
  paxos_seqnum_t accepted;
};

struct dhc_block {
  chordID id;
  ptr<keyhash_meta> meta;
  ptr<keyhash_data> data;
};

struct paxos_state_t {
  bool recon_inprogress;
  uint promise_recvd;
  uint accept_recvd;
  vec<chordID> acc_conf;
  
  paxos_state_t () : recon_inprogress(false), promise_recvd(0), accept_recvd(0) {}
  
  ~paxos_state_t () { acc_conf.clear ();}
};

struct dhc_soft {
  chordID id;
  u_int64_t config_seqnum;
  vec<ptr<location> > config;
  vec<ptr<location> > new_config;   //next accepted config
  paxos_seqnum_t proposal;
  paxos_seqnum_t promised;

  ptr<paxos_state_t> pstat;
  
  ihash_entry <dhc_soft> link;

  dhc_soft (ptr<vnode> myNode, ptr<dhc_block> kb)
  {
    id = kb->id;
    config_seqnum = kb->meta->config.seqnum;
    set_locations (config, myNode, kb->meta->config.nodes);
    proposal.seqnum = 0;
    bzero (&proposal.proposer, sizeof (chordID));    
    promised.seqnum = kb->meta->accepted.seqnum;
    bcopy (&kb->meta->accepted.proposer, &promised.proposer, sizeof (chordID));
    
    pstat = New refcounted<paxos_state_t>;
  }
  
  ~dhc_soft () 
  {
    config.clear ();
    new_config.clear ();
  }
};

class dhc {
  
  ptr<vnode> myNode;
  ptr<dbfe> db;
  ihash<chordID, dhc_soft, &dhc_soft::id, &dhc_soft::link, hashID> dhcs;

  uint n_replica;

  void recv_prepare (user_args *);
  void recv_promise (chordID, ref<dhc_prepare_res>, clnt_stat);
  void recv_propose (user_args *);
  void recv_accept (chordID, ref<dhc_propose_res>, clnt_stat);
  void recv_newconfig (user_args *);
  void recv_newconfig_ack (chordID, ref<dhc_newconfig_res>, clnt_stat);
  
 public:

  dhc (ptr<vnode>, str, uint, str);
  ~dhc () {};
  
  void recon (chordID);
  void dispatch (user_args *);
  
};

#endif /*_DHC_H_*/
