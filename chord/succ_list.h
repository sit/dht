#ifndef _SUCC_LIST_H_
#define _SUCC_LIST_H_

#define NSUCC    2*10     // 2 * log of # vnodes

class succ_list : public stabilizable {
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
  void stabilize_getpred_cb (chordID s, chordID p,
			     net_address r, chordstat status);
  void stabilize_getpred_cb_ok (chordID sd, 
				chordID p, bool ok, chordstat status);

  // Helpers for stabilize_succlist
  void stabilize_getsucclist_cb (chordID s, vec<chord_node> nlist,
				chordstat err);
  void stabilize_getsucclist_check (chordID src, chordID chk, chordstat status);
  void stabilize_getsucclist_ok (chordID source,
				 chordID ns, bool ok, chordstat status);


 public:  
  succ_list (ptr<vnode> v, ptr<locationtable> locs, chordID myID);
  
  chordID succ ();
  chordID operator[] (int n);
  
  int num_succ ();
  u_long estimate_nnodes ();
  void print ();
  
  void fill_getsuccres (chord_nodelistextres *res);
  
  void stabilize_succ ();
  void stabilize_succlist ();
  
  // Stabilizable methods
  bool backoff_stabilizing () { return nout_backoff > 0; }
  bool continuous_stabilizing () { return nout_continuous > 0; }
  void do_continuous () { stabilize_succ (); }
  void do_backoff () { stabilize_succlist (); }
  bool isstable () { return stable_succlist && stable_succlist2; } // XXX
};
#endif /* _SUCC_LIST_H_ */
