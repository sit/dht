#include "koorde.h"
#include "rpc.h"
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
  cout << "Koorde find_successor(" << me.id << "): " << key << endl;
  koorde_lookup_arg a;
  koorde_lookup_ret r;
  a.k = key;
  a.kshift = key;
  a.i = me.id;
  koorde_lookup (&a, &r);
  cout << "Koorde find_successor(" << me.id << "): " << key 
       << " = " << r.r.id << endl;
  vector<Chord::IDMap> succs;
  succs.push_back (r.r);
  return succs;
}

void
Koorde::stabilize (void *) 
{
  Chord::stabilize (NULL);
  cout << "Koorde::stabilzie\n";
  vector<IDMap> succs = find_successors (debruijn, 1);
  assert (succs.size () > 0);
  loctable->add_node(succs[0]);
}

Koorde::Koorde(Node *n) : Chord(n) {
  debruijn = me.id << 1;
  cout << "Koorde " << me.id << " debruijn " << debruijn << endl;
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
  IDMap succ = loctable->succ(1);
  printf ("Koorde (%qu) lookup key=%qx kshift=%qx i=%qx succ=%qu\n", 
	  me.id, a->k, a->kshift, a->i, succ.id);
  if (ConsistentHash::betweenrightincl (me.id, succ.id, a->k)) {
    r->r = succ;
  } else if (ConsistentHash::betweenrightincl (me.id, succ.id, a->i)) {
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

