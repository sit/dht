#ifndef _SUCC_LIST_H_
#define _SUCC_LIST_H_

#define NSUCC    16

class succ_list : public fingerlike {
  chordID myID;
  ptr<vnode> myvnode;
  ptr<locationtable> locations;
  
  u_long nnodes; // estimate of the number of chord nodes
  
  chordID oldsucc;  // last known successor to myID
  bool stable_succlist;
  bool stable_succlist2;
  u_int nout_backoff;
  u_int nout_continuous;

  // Helpers for stabilize_succ
  void stabilize_getpred_cb (chordID s, chord_node p, chordstat status);

  // Helpers for stabilize_succlist
  void stabilize_getsucclist_cb (chordID s, vec<chord_node> nlist,
				chordstat err);
  void stabilize_getsucclist_check (chordID src, chordID chk, chordstat status);

 public:  
  succ_list (ptr<vnode> v, ptr<locationtable> locs, chordID myID);
  
  chordID succ ();
  chordID operator[] (unsigned int n);
  
  unsigned int num_succ ();
  u_long estimate_nnodes ();
  
  void stabilize_succ ();
  void stabilize_succlist ();
  
  vec<chord_node> succs ();

  // Stabilizable methods
  bool backoff_stabilizing () { return nout_backoff > 0; }
  bool continuous_stabilizing () { return nout_continuous > 0; }
  void do_continuous () { stabilize_succ (); }
  void do_backoff () { stabilize_succlist (); }
  bool isstable () { return stable_succlist && stable_succlist2; } // XXX
  void fill_nodelistresext (chord_nodelistextres *res);
  void fill_nodelistres (chord_nodelistres *res);
  void print ();
  void stats () { warn << "stats go here\n"; };

  // Fingerlike methods
  chordID closestpred (const chordID &x);
  chordID closestpred (const chordID &x, vec<chordID> fail);
  chordID closestsucc (const chordID &x);
  void init (ptr<vnode> v, ptr<locationtable> locs, chordID ID) { warn << "not needed\n";};

  ref<fingerlike_iter> get_iter (); 
};
#endif /* _SUCC_LIST_H_ */
