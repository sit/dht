#ifndef _PRED_LIST_H_
#define _PRED_LIST_H_

// Predecessor lists are totally broken. DO NOT USE!
#define PRED_LIST

#include "stabilize.h"

/**
 * This class is analogous to the successor list.
 * However, for Chord correctness reasons, it uses a different
 * algorithm to find the predecessor list.  Instead of retrieving
 * a "predecessor list" from the predecessor, it does a (slow) lookup
 * for the predecessor of myID - x for some x such that
 *    $x \approx O(\log n / n )$.
 * Then there will be an expected $\Theta(\log n)$ nodes in front of
 * this node, so we retrieve that node's successor list.
 */
class pred_list : public stabilizable {
  chordID myID;
  ptr<vnode> v_;
  ptr<locationtable> locations;

#ifdef PRED_LIST
  int npred_;
  chordID backkey_;
#endif /* PRED_LIST */

  u_int nout_continuous;
  ptr<location> oldpred_; // used to check if predecessor is stable
  
  u_int nout_backoff;
  bool stable_predlist;

  void stabilize_pred ();
  void stabilize_getsucc_cb (chordID sd, chord_node s, chordstat status);

  void stabilize_predlist ();
  void stabilize_predlist_cb (vec<chord_node> nlist, chordstat status);

  void update_pred_fingers_cb (vec<chord_node> nlist, chordstat s);
  
 public:  
  pred_list (ptr<vnode> v, ptr<locationtable> locs);
  
  ptr<location> pred ();
  
  vec<ptr<location> > preds ();

  void update_pred (const chord_node &p);
  unsigned int num_pred ();

  // Stabilizable methods
  bool backoff_stabilizing () { return nout_backoff > 0; }
  bool continuous_stabilizing () { return nout_continuous > 0; }
  void do_continuous ();
  void do_backoff ();
  bool isstable ();
  void fill_nodelistresext (chord_nodelistextres *res);
  void fill_nodelistres (chord_nodelistres *res);
  
  ptr<location> closestpred (const chordID &x, vec<chordID> fail) ;
  ptr<location> closestsucc (const chordID &x);
};

#endif /* _PRED_LIST_H_ */
