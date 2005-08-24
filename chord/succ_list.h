#ifndef _SUCC_LIST_H_
#define _SUCC_LIST_H_

#include "stabilize.h"

class succ_list : public stabilizable {
  chordID myID;
  ptr<vnode> myvnode;
  ptr<locationtable> locations;
  
  u_long nnodes; // estimate of the number of chord nodes

  int nsucc_; // number of successors to maintain
  
  ptr<location> oldsucc;  // last known successor
  bool stable_succlist;
  bool stable_succlist2;
  u_int nout_backoff;
  u_int nout_continuous;

  // Helpers for stabilize_succ
  void stabilize_getpred_cb (ptr<location> sd, chord_node p, chordstat status);

  // Helpers for stabilize_succlist
  void stabilize_getsucclist_cb (ptr<location> s, vec<chord_node> nlist,
				 chordstat err);
  void stabilize_getsucclist_check (ptr<location> src, chordID chk,
				    chordstat status);

 public:  
  succ_list (ptr<vnode> v, ptr<locationtable> locs);
  
  ptr<location> succ ();
  
  unsigned int num_succ ();
  u_long estimate_nnodes ();
  
  void stabilize_succ ();
  void stabilize_succlist ();
  
  vec<ptr<location> > succs ();

  // Stabilizable methods
  bool backoff_stabilizing () { return nout_backoff > 0; }
  bool continuous_stabilizing () { return nout_continuous > 0; }
  void do_continuous () { stabilize_succ (); }
  void do_backoff () { stabilize_succlist (); }
  bool isstable () { return stable_succlist && stable_succlist2; } // XXX
  void fill_nodelistresext (chord_nodelistextres *res);
  void fill_nodelistres (chord_nodelistres *res);

  void print (strbuf &out);

  ptr<location> closestpred (const chordID &x, vec<chordID> fail);
};
#endif /* _SUCC_LIST_H_ */
