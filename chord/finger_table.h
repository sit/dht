#include "fingerlike.h"

#ifndef _FINGER_TABLE_H_
#define _FINGER_TABLE_H_

#define NBIT     160     // size of Chord identifiers in bits
class finger_table : public fingerlike {
  ptr<vnode> myvnode;
  ptr<locationtable> locations;
  
  chordID starts[NBIT];
  chordID fingers[NBIT]; // just for optimizing stabilization
  chordID myID;

  int f; // next finger to stabilize
  bool stable_fingers;
  bool stable_fingers2;
  
  u_int nout_backoff;
  
  u_long nslowfinger;
  u_long nfastfinger;

  void stabilize_finger_getpred_cb (chordID dn, int i, chordID p, 
				    net_address r, chordstat status);
  void stabilize_findsucc_cb (chordID dn,
			      int i, chordID s, route path, chordstat status);

  
 public:
  finger_table ();

  chordID finger (int i);
  chordID operator[] (int i);
  chordID start (int i) { return starts[i]; }

  void stabilize_finger ();

  // Stabilize methods
  bool backoff_stabilizing () { return nout_backoff > 0; }
  void do_backoff () { stabilize_finger (); }
  bool isstable () { return stable_fingers && stable_fingers2; }
  void print ();
  void fill_nodelistres (chord_nodelistres *res);
  void fill_nodelistresext (chord_nodelistextres *res);
  void stats ();

  //fingerlike methods
  void init (ptr<vnode> v, ptr<locationtable> locs, chordID ID);
  chordID closestpred (const chordID &x);
  chordID closestsucc (const chordID &x);

};

#endif /* _FINGER_TABLE_H_ */
