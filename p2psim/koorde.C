#include "koorde.h"
#include <stdio.h>
#include <iostream>
using namespace std;

static uint
topbit (Chord::CHID n)
{
  uint r = n > 31;
  return r;
}

vector<Chord::IDMap>
Koorde::find_successors(CHID key, int m)
{
  cout << "Koorde find_successor" << endl;
  koorde_lookup_arg a;
  koorde_lookup_ret r;
  a.k = key;
  a.kshift = key;
  a.i = me.id;
  koorde_lookup (&a, &r);
  vector<Chord::IDMap> succs;
  succs.push_back (r.r);
}

Koorde::Koorde(Node *n) : Chord(n) {
  cout << "Koorde " << me.id << endl;
  debruijn = me.id << 1;
};

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
  printf ("Koorde (%u) lookup key=%u kshift=%u i=%u succ=%u\n", 
	  me.id, a->k, a->kshift, a->i, succ.id);
  if (ConsistentHash::between (me.id, succ.id, a->k)) {
    r->r = succ;
  } else if (ConsistentHash::between (me.id, succ.id, a->i)) {
    koorde_lookup_arg na;
    koorde_lookup_ret nr;
    IDMap d = loctable->pred (debruijn);
    na.k = a->k;
    na.kshift = a->kshift << 1;
    na.i = nextimagin (a->i, a->kshift);
    doRPC (d.ip, &Koorde::koorde_lookup, &na, &nr);
    r->r = nr.r;
  } else {
    koorde_lookup_arg na = *a;
    koorde_lookup_ret nr;
    doRPC (succ.ip,  &Koorde::koorde_lookup, &na, &nr);
    r->r = nr.r;
  }
}

