#ifndef _FINGER_TABLE_H_
#define _FINGER_TABLE_H_

#include "stabilize.h"

class finger_table : public stabilizable {
  ptr<vnode> myvnode;
  ptr<locationtable> locations;
  
  chordID starts[NBIT];
  ptr<location> fingers[NBIT]; // just for optimizing stabilization
  chordID myID;

  int f; // next finger to stabilize
  bool stable_fingers;
  bool stable_fingers2;
  
  u_int nout_backoff;
  
  u_long nslowfinger;
  u_long nfastfinger;

  void stabilize_finger_getpred_cb (chordID dn, int i, chord_node p,
				    chordstat status);
  void stabilize_findsucc_cb (chordID dn, int i, vec<chord_node> succs,
			      route path, chordstat status);

  
 public:
  finger_table (ptr<vnode> v, ptr<locationtable> l);

  ptr<location> finger (int i);
  ptr<location> operator[] (int i);
  chordID start (int i) { return starts[i]; }
  
  vec<ptr<location> > get_fingers ();

  void stabilize_finger ();

  void print (strbuf &outbuf);
  void stats ();
  
  // Stabilize methods
  bool backoff_stabilizing () { return nout_backoff > 0; }
  void do_backoff () { stabilize_finger (); }
  bool isstable () { return stable_fingers && stable_fingers2; }

  void fill_nodelistres (chord_nodelistres *res);
  void fill_nodelistresext (chord_nodelistextres *res);

  ptr<location> closestpred (const chordID &x, vec<chordID> fail);
};

#endif /* _FINGER_TABLE_H_ */
