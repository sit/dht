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
  //vec<chordID> nodes;
};

struct proposal_t {
  paxos_seqnum_t proposal_num;
  u_int64_t cur_config_seqnum;
  ptr<replica_t> new_config;
};

struct paxos_state_t {
  bool recon_inprogress;
  uint promise_recvd;
  vec<chordID> acc_conf;
  uint accept_recvd;
};

struct keyhash_meta {
  ptr<replica_t> config;
  ptr<replica_t> new_config;   //next accepted config
  paxos_seqnum_t proposal;     //proposal number
  paxos_seqnum_t promised;     //promised number
  paxos_seqnum_t accepted;     //latest accepted proposal number
  //ptr<proposal_t> accepted;    //accepted proposal (do we need this var?)

  paxos_state_t pstat;
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
  void recv_propose ();
  void recv_accept (ptr<dhc_block>, ref<dhc_propose_res>, clnt_stat);
  void recv_newconfig ();
  void recv_newconfig_ack (ptr<dhc_block>, ref<dhc_newconfig_res>, clnt_stat);
  
 public:

  dhc (ptr<vnode>, str, uint, str);
  ~dhc () {};
  
  void recon (chordID);
  void dispatch (user_args *);
  
};

#endif /*_DHC_H_*/
