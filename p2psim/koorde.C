#include "koorde.h"
#include <stdio.h>
#include <iostream>
using namespace std;

Koorde::Koorde(Node *n) : Chord(n) 
{
  debruijn = me.id << 1;
  d = me;
  printf ("Koorde: (%u,%qx) debruijn=%qx\n", me.ip, me.id, debruijn);
}

// Iterative version of the figure 2 algo in IPTPS'03 paper.
vector<Chord::IDMap>
Koorde::find_successors(CHID key, int m)
{
  int count = 0;
  koorde_lookup_arg a;
  koorde_lookup_ret r;

  r.next = me;
  r.k = key;
  r.kshift = key;
  r.i = me.id + 1;

  printf ("find_successor(ip %u,id %qx,key %qx)\n", me.ip, me.id, key);

  while (1) {
    assert (count++ < 1000);

    a.k = r.k;
    a.kshift = r.kshift;
    a.i = r.i;
    last = r.next;

    printf("nexthop (%u,%qx) is (%u,%qx) key %qx i %qx, kshift %qx)\n", 
	   me.ip, me.id, r.next.ip, r.next.id, r.k, r.kshift, r.i);

    doRPC (r.next.ip, &Koorde::koorde_next, &a, &r);

    if (r.done) break;
  }

  printf ("find_successor for (id %qx, key %qx) is (%u,%qx) hops %d\n", 
	  me.id, key, r.next.ip, r.next.id, count);

  vector<Chord::IDMap> succs;
  succs.push_back (r.next);
  return succs;
}

void
Koorde::koorde_next(koorde_lookup_arg *a, koorde_lookup_ret *r)
{
  IDMap succ = loctable->succ(1);
  printf ("Koorde_next (id=%qx, key=%qx, kshift=%qx, i=%qx) succ=(%u,%qx)\n", 
	  me.id, a->k, a->kshift, a->i, succ.ip, succ.id);
  if (ConsistentHash::betweenrightincl (me.id, succ.id, a->k) ||
      me.id == succ.id) {
    r->next = succ;
    r->k = a->k;
    r->kshift = a->kshift;
    r->i = a->i;
    r->done = true;
    printf ("Koorde_next: done succ key = %qx: (%u,%qx)\n", 
	    a->k, succ.ip, succ.id);
  } else if (ConsistentHash::betweenrightincl (me.id, succ.id, a->i)) {
    r->next = d;
    r->k = a->k;
    r->kshift = a->kshift << 1;
    r->i = nextimagin (a->i, a->kshift);
    r->done = false;
    printf ("Koorde_next: contact de bruijn finger (%u,%qx)\n", d.ip, d.id);
  } else {
    printf ("Koorde_next: contact successor (%u,%qx)\n", succ.ip, succ.id);
    r->next = succ;
    r->k = a->k;
    r->kshift = a->kshift;
    r->i = a->i;
    r->done = false;
  }
}

void
Koorde::fix_debruijn () 
{
  cout << "fix_debruijn\n";
  vector<IDMap> succs = find_successors (debruijn, 1);
  printf ("fix_debruijn (%u,%qx): debruijn %qx succ %qx d %qx\n",
	  me.ip, me.id, debruijn, succs[0].id, last.id);
  assert (succs.size () > 0);
  d = last;
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
Koorde::stabilized (vector<ConsistentHash::CHID> lid)
{
  bool r = Chord::stabilized (lid);

  if (!r) return false;

  printf ("koorde::stabilized? me %qx debruijn %qx d %qx\n", me.id,
	  debruijn, d.id);

  assert (lid.size () > 0);

  if (lid.size () == 1) {
    assert (d.id == lid.front ());
    return true;
  } else {
    for (uint i = 0; i < lid.size () - 1; i++) {
      //printf ("stable? %qx %qx %qx, %qx\n", lid[i], lid[i+1], d.id, debruijn);
      if (ConsistentHash::betweenrightincl (lid[i], lid[i+1], debruijn)) {
	r = (lid[i] == d.id);
	if (!r) {
	  printf ("loop: not stable: d should be %qx but is %qx\n", 
		  lid[i], d.id);
	}
	return r;
      }
    }
    printf ("stable? post %qx %qx %qx, %qx\n", lid.front (), lid.back (), 
	    d.id, debruijn);
    if (ConsistentHash::betweenrightincl (lid.back (), lid.front (), 
					  debruijn)) {
      r = (lid.back () == d.id);
      if (!r) {
	printf ("post: not stable: d should be %qx but is %qx\n", 
		lid.back (), d.id);
      }
      return r;
    }
  }
  
  return false;
}

void
Koorde::dump ()
{
  Chord::dump ();
  printf ("Koorde (%u,%qx) debruijn %qx d (%u,%qx)\n", me.ip, me.id, debruijn,
	  d.ip, d.id);
	  
}
