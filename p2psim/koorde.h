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
  static const uint k = 2;  // k-degree de bruijn graph; 
  static const uint logbase = k >> 1;  // degree k = 2 ^ logbase
  Chord::CHID debruijn;  // = k * me
  vector<IDMap> dfingers;  // predecessor(debruijn) + k - 1 successors
  IDMap last;
  bool isstable;

  static Chord::CHID Koorde::nextimagin (CHID i, CHID kshift) {
    uint t = ConsistentHash::getbit (kshift, NBCHID - logbase, logbase);
    CHID r = i << logbase | t;
    // printf ("nextimagin: kshift %qx topbit is %u, i is %qx new i is %qx\n",
    //  kshift, t, i, r);
    return r;
  }
  Chord::CHID Koorde::firstimagin (CHID, CHID, CHID, CHID*);
  IDMap Koorde::closestpreddfinger (CHID);


  vector<Chord::IDMap> Koorde::find_successors(CHID key, int m);
  void fix_debruijn();
  void stabilize();
  void reschedule_stabilizer(void *x);
};

#endif // __KOORDE_H
