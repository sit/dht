#ifndef __KOORDE_H
#define __KOORDE_H

#include "chord.h"

// Koorde extends base Chord with debruijn routing
class Koorde : public Chord {
public:
  Koorde(Node *n);
  ~Koorde() {};

  struct koorde_lookup_arg {
    CHID k;
    CHID kshift;
    CHID i;
  };

  struct koorde_lookup_ret {
    IDMap next;
    CHID k;
    CHID kshift;
    CHID i;
    bool done;
  };

  // RPC handlers
  void koorde_next (koorde_lookup_arg *, koorde_lookup_ret *);

  bool stabilized(vector<ConsistentHash::CHID>);
  void dump();

protected:
  Chord::CHID debruijn;
  IDMap d;
  IDMap last;

  static Chord::CHID Koorde::nextimagin (CHID i, CHID kshift) {
    uint t = ConsistentHash::topbit (kshift);
    CHID r = i << 1 | t;
    // printf ("nextimagin: kshift %qx topbit is %u, i is %qx new i is %qx\n",
    //  kshift, t, i, r);
    return r;
  }

  vector<Chord::IDMap> Koorde::find_successors(CHID key, int m);
  void fix_debruijn();
  void stabilize();
  void reschedule_stabilizer(void *x);
};

#endif // __KOORDE_H
