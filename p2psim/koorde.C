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

Koorde::Koorde(Node *n) : Chord(n) 
{
  debruijn = me.id << 1;
  printf ("Koorde: %u, id=%qx debruijn=%qx\n", me.ip, me.id, debruijn);
  loctable->resize((3 + CHORD_SUCC_NUM), CHORD_SUCC_NUM);
  // loctable->pin(debruijn);
};

Chord::CHID
Koorde::nextimagin (CHID i, CHID kshift)
{
  uint t = topbit (kshift);
  uint r = i << 1 | t;
  return r;
}

vector<Chord::IDMap>
Koorde::find_successors(CHID key, int m)
{
  printf ("Koorde find_successor(ip %u,id %qx,key %qx)\n", me.ip, me.id, key);

  koorde_lookup_arg a;
  koorde_lookup_ret r;
  a.k = key;
  a.kshift = key;
  a.i = me.id;
  koorde_lookup (&a, &r);

  printf ("Koorde find_successor for (id %qx, key %qx) is %u,%qx\n", 
	  me.id, key, r.r.ip, r.r.id);
  vector<Chord::IDMap> succs;
  succs.push_back (r.r);
  return succs;
}

void
Koorde::koorde_lookup(koorde_lookup_arg *a, koorde_lookup_ret *r)
{
  IDMap succ = loctable->succ(1);
  printf ("Koorde_lookup (id=%qx, key=%qx, kshift=%qx, i=%qx) succ=%u,%qx\n", 
	  me.id, a->k, a->kshift, a->i, succ.ip, succ.id);
  if (ConsistentHash::betweenrightincl (me.id, succ.id, a->k) ||
      me.id == succ.id) {
    r->r = succ;
    printf ("Koorde_lookup: done succ key = %qx: %u %qx\n", 
	    a->k, succ.ip, succ.id);
  } else if (ConsistentHash::betweenrightincl (me.id, succ.id, a->i)) {
    koorde_lookup_arg na;
    koorde_lookup_ret nr;
    IDMap d = loctable->pred (debruijn);
    na.k = a->k;
    na.kshift = a->kshift << 1;
    na.i = nextimagin (a->i, a->kshift);
    printf ("Koorde_lookup: contact de bruijn finger (%u %qx)\n", d.ip, d.id);
    doRPC (d.ip, &Koorde::koorde_lookup, &na, &nr);
    r->r = nr.r;
  } else {
    koorde_lookup_arg na = *a;
    koorde_lookup_ret nr;
    printf ("Koorde_lookup: contact successor (%u %qx)\n", succ.ip, succ.id);
    doRPC (succ.ip,  &Koorde::koorde_lookup, &na, &nr);
    r->r = nr.r;
  }
}


void
Koorde::fix_debruijn () 
{
  cout << "Koorde::fix_debruijn\n";
  vector<IDMap> succs = find_successors (debruijn, 1);
  assert (succs.size () > 0);
  //  loctable->add_node(succs[0]);
}

void
Koorde::reschedule_stabilizer(void *x)
{
  Koorde::stabilize();
  delaycb(STABLE_TIMER, &Koorde::reschedule_stabilizer, (void *)0);
}

void
Koorde::stabilize()
{
  Chord::stabilize();
  fix_debruijn();
}

bool
Koorde::stabilized ()
{
  printf ("koorde::stabilized\n");
  bool r = Chord::stabilized ();
  return r;
}

void
Koorde::dump ()
{
  IDMap d = loctable->pred (debruijn);
  Chord::dump ();
  printf ("Koorde (%u,%qx) debruijn %qx d (%u, %qx)\n", me.ip, me.id, debruijn,
	  d.ip, d.id);
	  
}
