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
  int promise_recvd;
  int accept_ack_recvd;
};

struct keyhash_meta {
  ptr<replica_t> config;
  ptr<replica_t> new_config;   //next accepted config
  paxos_seqnum_t proposal; //proposal number
  paxos_seqnum_t promised; //promised number
  ptr<proposal_t> accepted;     //accepted proposal

  paxos_state_t pstat;
};

struct dhc_block {
  ptr<keyhash_data> data;
  ptr<keyhash_meta> meta;
};

class dhc {
  
  ptr<vnode> myNode;
  ptr<dbfe> db;
  ptr<aclnt> dhcclnt;

  void recv_prepare ();
  void recv_promise (ptr<dhc_block>, ref<dhc_prepare_res>, clnt_stat);
  void recv_accept ();
  void recv_accept_ack ();
  void recv_newconfig ();
  
 public:

  dhc (ptr<vnode>, str, str);
  ~dhc () {};
  
  void recon (chordID);
  
};

// misc functions

static void open_db (ptr<dbfe>, str, dbOptions, str);

#endif /*_DHC_H_*/
