#ifndef __KOORDE_H
#define __KOORDE_H

#include "chord.h"

// Koorde extends base Chord with k-degree debruijn routing
class Koorde : public Chord {
public:
  Koorde(Node *n, Args& a);
  ~Koorde() {};
  string proto_name() { return "Koorde"; }

  struct koorde_lookup_arg {
    CHID k;
    CHID kshift;
    CHID i;
    uint nsucc;
  };

  struct koorde_lookup_ret {
    IDMap next;
    CHID k;
    CHID kshift;
    CHID i;
    bool done;
    vector<IDMap> v;
  };

  // RPC handlers
  void koorde_next (koorde_lookup_arg *, koorde_lookup_ret *);

  bool stabilized(vector<ConsistentHash::CHID>);
  vector<Chord::IDMap> Koorde::find_successors(CHID key, uint m, bool intern, bool is_lookup = false);
  void init_state(vector<IDMap> ids);
  void dump();

protected:
  uint logbase;  // log degree
  uint k;  // k-degree de bruijn graph; 
  uint nsucc;
  uint fingers;   // number of successors of debruijn finger
  uint resilience;  // resilience

  Chord::CHID debruijn;  // = k * me
  Chord::CHID debruijnpred;  // = k * me - x

  IDMap last;
  bool isstable;
  vector<IDMap> lastdfingers;
  
  Chord::CHID Koorde::nextimagin (CHID i, CHID kshift);
  Chord::CHID Koorde::firstimagin (CHID, CHID, CHID, CHID*);
  IDMap Koorde::closestpreddfinger (CHID);

  void fix_debruijn();
  void stabilize();
  void reschedule_stabilizer(void *x);
  bool debruijn_stabilized (ConsistentHash::CHID finger, uint n,
			    vector<ConsistentHash::CHID> lid);
  void debruijn_dump (Chord::CHID finger, uint n);

};

#endif // __KOORDE_H
