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
    IDMap r;
  };

  // RPC handlers
  void koorde_lookup (koorde_lookup_arg *, koorde_lookup_ret *);

protected:
  Chord::CHID debruijn;

  Chord::CHID nextimagin (CHID i, CHID kshift);

  vector<Chord::IDMap> Koorde::find_successors(CHID key, int m);

  void fix_debruijn();
  void stabilize();
  void reschedule_stabilizer(void *x);

};

#endif // __KOORDE_H
