#ifndef _FINGER_TABLE_H_
#define _FINGER_TABLE_H_

#include "stabilize.h"

class finger_table : public stabilizable {

protected:
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

  
  finger_table (ptr<vnode> v, ptr<locationtable> l);
 public:
  
  static ptr<finger_table> produce_finger_table (ptr<vnode> v, ptr<locationtable> l);

  virtual ptr<location> finger (int i);
  virtual ptr<location> operator[] (int i);
  chordID start (int i) { return starts[i]; }
  
  vec<ptr<location> > get_fingers ();

  virtual void stabilize_finger ();

  void print (strbuf &outbuf);
  virtual void stats ();
  
  // Stabilize methods
  virtual bool backoff_stabilizing () { return nout_backoff > 0; }
  virtual void do_backoff () { stabilize_finger (); }
  virtual bool isstable () { return stable_fingers && stable_fingers2; }

  virtual void fill_nodelistres (chord_nodelistres *res);
  virtual void fill_nodelistresext (chord_nodelistextres *res);

  virtual ptr<location> closestpred (const chordID &x, vec<chordID> fail);
};

#endif /* _FINGER_TABLE_H_ */
