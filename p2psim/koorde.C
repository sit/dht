#include "koorde.h"
#include <iostream>
using namespace std;

static uint
topbit (Chord::CHID n)
{
  uint r = n > 31;
  return r;
}

Chord::CHID
Koorde::nextimagin (CHID i, CHID kshift)
{
  uint t = topbit (kshift);
  uint r = i << 1 | t;
  return r;
}

void
Koorde::koorde_lookup(koorde_lookup_arg *a, koorde_lookup_ret *r)
{
  IDMap succ = loctable->succ(me.id);
  if (ConsistentHash::between (a->k, me.id, succ.id)) {
    r->r = succ;
  } else if (ConsistentHash::between (a->i, me.id, succ.id)) {
    koorde_lookup_arg na;
    koorde_lookup_ret nr;
    na.k = a->k;
    na.kshift = a->kshift << 1;
    na.i = nextimagin (a->i, a->kshift);
    doRPC (debruijn.ip, &Koorde::koorde_lookup, &na, &nr);
    r->r = nr.r;
  } else {
    koorde_lookup_arg na = *a;
    koorde_lookup_ret nr;
    doRPC (succ.ip,  &Koorde::koorde_lookup, &na, &nr);
    r->r = nr.r;
  }
}

