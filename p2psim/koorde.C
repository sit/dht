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
  d = me;
  printf ("Koorde: (%u,%qx) debruijn=%qx\n", me.ip, me.id, debruijn);
}

Chord::CHID
Koorde::nextimagin (CHID i, CHID kshift)
{
  uint t = topbit (kshift);
  uint r = i << 1 | t;
  return r;
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
  r.i = me.id;

  printf ("find_successor(ip %u,id %qx,key %qx)\n", me.ip, me.id, key);

  while (1) {
    assert (count++ < 100);

    a.k = r.k;
    a.kshift = r.kshift;
    a.i = r.i;
    last = r.next;

    printf("nexthop (%u,%qx) is (%u,%qx) key %qx i %qx, kshift %qx)\n", 
	   me.ip, me.id, r.next.ip, r.next.id, r.k, r.kshift, r.i);

    doRPC (r.next.ip, &Koorde::koorde_next, &a, &r);

    if (r.done) break;
  }

  printf ("find_successor for (id %qx, key %qx) is (%u,%qx)\n", 
	  me.id, key, r.next.ip, r.next.id);

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
    printf ("Koorde_lookup: contact successor (%u,%qx)\n", succ.ip, succ.id);
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
  bool r = false;
  printf ("koorde::stabilized? me %qx debruijn %qx d %qx\n", me.id,
	  debruijn, d.id);

  assert (lid.size () > 0);

  vector<ConsistentHash::CHID>::iterator i = lid.begin ();
  vector<ConsistentHash::CHID>::iterator j = lid.begin ();
  if (lid.size () == 1) {
    assert (d.id == *i);
    return true;
  } else {
    for (++j; j != lid.end(); ++i,++j) {
      printf ("stable %qx %qx %qx, %qx?\n", *i, *j, d.id, debruijn);
      if (ConsistentHash::betweenrightincl (*i, *j, debruijn)) {
	r = (*i == d.id);
	if (!r) {
	  printf ("not stable: d should be %qx but is %qx\n", *i, d.id);
	}
	return r;
      }
    }
    i = lid.begin ();
    if (ConsistentHash::betweenrightincl (*j, *i, debruijn)) {
      r = (*j == d.id);
      if (!r) {
	printf ("not stable: d should be %qx but is %qx\n", *j, d.id);
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
