#include "koorde.h"
#include <stdio.h>
#include <iostream>
using namespace std;

Koorde::Koorde(Node *n) : Chord(n) 
{
  debruijn = me.id << 1;
  d = me;
  isstable = false;
  printf ("Koorde: (%u,%qx) debruijn=%qx\n", me.ip, me.id, debruijn);
}

  
// Create an imaginary node with as many bits from k as possible and
// such that start < i <= succ.
Chord::CHID 
Koorde::firstimagin (CHID start, CHID succ, CHID k, CHID *kr) 
{
  Chord::CHID i;

  if (start == succ) i = start;  // XXX yuck
  else {
    int bm = ConsistentHash::bitposmatch (start, succ) - 1;
    int bs = bm;

    // skip the first 0 bit in start; result will be smaller than succ
    for ( ; bs >= 0; bs--) {
      if (ConsistentHash::getbit (start, bs) == 0)
	break;
    }
    bs--;

    // skip till the next 0 bit in start;
    for ( ; bs >= 0; bs--) {
      if (ConsistentHash::getbit (start, bs) == 0)
	break;
    }

    i = ConsistentHash::setbit (start, bs, 1);
    bs--;
    if (bs >= 0) {
      // slap top bits from k into i at pos bs
      ConsistentHash::CHID mask = (((CHID) 1) << (bs+1)) - 1;
      mask = ~mask;
      i = i & mask;
      ConsistentHash::CHID bot = k >> (NBCHID - bs - 1);
      i = i | bot;
      
      // shift bs top bits out k
      *kr = k << (bs + 1);
      // printf ("start %qx succ %qx i %qx k %qx bs %d kr %qx\n",
      //     start, succ, i, k, bs, *kr);
    } else {
      *kr = k;
    }
    assert (ConsistentHash::betweenrightincl (start, succ, i));
  }
  return i;
}


// Iterative version of the figure 2 algo in IPTPS'03 paper.
vector<Chord::IDMap>
Koorde::find_successors(CHID key, int m)
{
  int count = 0;
  koorde_lookup_arg a;
  koorde_lookup_ret r;
  vector<ConsistentHash::CHID> path;
  IDMap mysucc = loctable->succ(1);

  r.next = me;
  r.k = key;
  r.i = firstimagin (me.id, mysucc.id, key, &r.kshift);

  printf ("find_successor(ip %u,id %qx, key %qx)\n", me.ip, me.id, key);

  while (1) {
    assert (count++ < 1000);

    a.k = r.k;
    a.kshift = r.kshift;
    a.i = r.i;
    last = r.next;

    printf("nexthop (%u,%qx) is (%u,%qx) key %qx i %qx, kshift %qx)\n", 
	   me.ip, me.id, r.next.ip, r.next.id, r.k, r.kshift, r.i);

    doRPC (r.next.ip, &Koorde::koorde_next, &a, &r);
    
    path.push_back (r.next.id);

    if (r.done) break;
  }

  printf ("find_successor for (id %qx, key %qx) is (%u,%qx) hops %d\n", 
	  me.id, key, r.next.ip, r.next.id, count);
  for (uint i = 0; i < path.size () - 1; i++) {
    printf ("  %qx\n", path[i]);
  }

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
  printf ("fix_debruijn %u\n", isstable);
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
  printf ("stabilize()? %u\n", isstable);
  if (!isstable) {
    Chord::stabilize();
    fix_debruijn();
  }
}

bool
Koorde::stabilized (vector<ConsistentHash::CHID> lid)
{
  bool r = Chord::stabilized (lid);

  if (!r) return false;
  r = false;

  printf ("koorde::stabilized? me %qx debruijn %qx d %qx\n", me.id,
	  debruijn, d.id);

  assert (lid.size () > 0);

  if (lid.size () == 1) {
    assert (d.id == lid.front ());
    r = true;
  } else {
    for (uint i = 0; i < lid.size () - 1; i++) {
      //printf ("stable? %qx %qx %qx, %qx\n", lid[i], lid[i+1], d.id, debruijn);
      if (ConsistentHash::betweenrightincl (lid[i], lid[i+1], debruijn)) {
	r = (lid[i] == d.id);
	if (!r) {
	  printf ("loop: not stable: d should be %qx but is %qx\n", 
		  lid[i], d.id);
	}
	r = true;
	break;
      }
    }
    if (!r) {
      printf ("stable? post %qx %qx %qx, %qx\n", lid.front (), lid.back (), 
	      d.id, debruijn);
      if (ConsistentHash::betweenrightincl (lid.back (), lid.front (), 
					    debruijn)) {
	r = (lid.back () == d.id);
	if (!r) {
	  printf ("post: not stable: d should be %qx but is %qx\n", 
		  lid.back (), d.id);
	}
	r = true;
      }
    }
  }
  
  isstable = r;
  return r;
}

void
Koorde::dump ()
{
  Chord::dump ();
  printf ("Koorde (%u,%qx) debruijn %qx d (%u,%qx)\n", me.ip, me.id, debruijn,
	  d.ip, d.id);
	  
}
