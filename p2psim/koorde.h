#ifndef __KOORDE_H
#define __KOORDE_H

#include "chord.h"

// Koorde extends base Chord with k-degree debruijn routing
class Koorde : public Chord {
public:
  Koorde(Node *n);
  ~Koorde() {};

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
  void dump();

protected:
  static const uint logbase = 16;
  static const uint k = 1 << logbase;  // k-degree de bruijn graph; 

  Chord::CHID debruijn;  // = k * me
  // vector<IDMap> dfingers;  // predecessor(debruijn) + k - 1 successors
  IDMap last;
  bool isstable;

  Chord::CHID Koorde::nextimagin (CHID i, CHID kshift);
  Chord::CHID Koorde::firstimagin (CHID, CHID, CHID, CHID*);
  IDMap Koorde::closestpreddfinger (CHID);

  vector<Chord::IDMap> Koorde::find_successors(CHID key, uint m, bool intern);
  void fix_debruijn();
  void stabilize();
  void reschedule_stabilizer(void *x);
};

#endif // __KOORDE_H
