#ifndef _DHC_H_
#define _DHC_H_

#include "dhash_impl.h"
#include <dbfe.h>

// PK blocks data structure for maintaining consistency.

struct tag_t {
  long version;
  chordID writer;
};

struct replica_t {
  long seqnum;
  vec<chordID> nodes;
};

struct paxos_seqnum_t {
  long seqnum; /* The latest number used by the previous proposer used, plus one */
  chordID proposer;
};

struct proposal_t {
  paxos_seqnum_t proposal_num;
  replica_t cur_config;
  replica_t new_config;
};

struct keyhash_meta {
  tag_t tag;
  replica_t config;
  paxos_seqnum_t promised;
  proposal_t accepted;
};

class dhc {

  //Global proposal number that this node has issued.
  //paxos_seqnum_t proposal_num; 

  ptr<dbfe> keyhash_rep_db;

  
};

#endif /*_DHC_H_*/
