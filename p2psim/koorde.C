#include "koorde.h"
#include <stdio.h>
#include <iostream>
using namespace std;

Koorde::Koorde(Node *n) : Chord(n, k) 
{
  debruijn = me.id << logbase;
  isstable = false;
  printf ("Koorde: (%u,%qx) debruijn=%qx\n", me.ip, me.id, debruijn);
  dfingers.push_back(me);
}

  
// Create an imaginary node i with as many bits from k as possible and
// such that start < i <= succ.
Chord::CHID 
Koorde::firstimagin (CHID start, CHID succ, CHID k, CHID *kr) 
{
  Chord::CHID i;

  if (start == succ) {  // XXX yuck
    i = start;
    *kr = k;
  } else {
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

    // set that bit to 1, now start < i <= succ holds
    i = ConsistentHash::setbit (start, bs, 1);
    bs--;
    
    int mod = bs % logbase;
    bs = bs - mod - 1;
    
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

Chord::IDMap
Koorde::closestpreddfinger (CHID n)
{
  assert (dfingers.size () > 0);

  if (dfingers.size () == 1) {
    return dfingers[0];
  }
  for (uint i = 0; i < dfingers.size (); i++) {
    if (ConsistentHash::betweenrightincl (dfingers[i].id, 
					  dfingers[(i+1) % dfingers.size ()].id,
					  n)) {
      return dfingers[i];
    }
  }
  return dfingers[dfingers.size () - 1];
}

// Iterative version of the figure 2 algo in IPTPS'03 paper.
vector<Chord::IDMap>
Koorde::find_successors(CHID key, uint m, bool intern)
{
  int count = 0;
  koorde_lookup_arg a;
  koorde_lookup_ret r;
  vector<ConsistentHash::CHID> path;
  IDMap mysucc = loctable->succ(1);

  a.nsucc = m;
  a.k = key;
  r.next = me;
  r.k = key;
  r.i = firstimagin (me.id, mysucc.id, a.k, &r.kshift);

  printf ("find_successor(ip %u,id %qx, key %qx)\n", me.ip, me.id, a.k);

  while (1) {
    assert (count++ < 1000);

    a.kshift = r.kshift;
    a.i = r.i;
    last = r.next;

    printf("nexthop (%u,%qx) is (%u,%qx) key %qx i %qx, kshift %qx)\n", 
	   me.ip, me.id, r.next.ip, r.next.id, a.k, r.i, r.kshift);

    doRPC (r.next.ip, &Koorde::koorde_next, &a, &r);
    
    path.push_back (r.next.id);

    if (r.done) break;
  }

    printf ("find_successor for (id %qx, key %qx) is (%u,%qx) hops %d\n", 
	    me.id, key, r.next.ip, r.next.id, count);
    for (uint i = 0; i < path.size () - 1; i++) {
      printf ("  %qx\n", path[i]);
    }

  assert (r.v.size () > 0);
  return r.v;
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
    r->v.clear ();
    assert (a->nsucc <= CHORD_SUCC_NUM);
    r->v = loctable->succs(a->nsucc);
    assert (r->v.size () > 0);
    printf ("Koorde_next: done succ key = %qx: (%u,%qx)\n", 
	    a->k, succ.ip, succ.id);
  } else if (ConsistentHash::betweenrightincl (me.id, succ.id, a->i)) {
    assert (dfingers.size () > 0);
    r->k = a->k;
    r->kshift = a->kshift << logbase;
    r->i = nextimagin (a->i, a->kshift);
    r->next = closestpreddfinger (r->i);
    r->done = false;
    printf ("Koorde_next: contact de bruijn finger (%u,%qx)\n", r->next.ip, 
	    r->next.id);
  } else {
    printf ("Koorde_next: contact successor (%u,%qx)\n", succ.ip, succ.id);
    r->k = a->k;
    r->next = loctable->pred (a->i); // succ
    r->kshift = a->kshift;
    r->i = a->i;
    r->done = false;
  }
}

void
Koorde::fix_debruijn () 
{
  printf ("fix_debruijn %u\n", isstable);
  vector<IDMap> succs = find_successors (debruijn, k - 1, true);
  printf ("fix_debruijn (%u,%qx): debruijn %qx succ %qx d %qx\n",
	  me.ip, me.id, debruijn, succs[0].id, last.id);
  assert (succs.size () > 0);
  succs.insert (succs.begin(), last);
  dfingers.clear ();
  dfingers = succs;
  for (uint i = 0; i < dfingers.size (); i++) {
    printf ("  Koorde %d debruijn fingers %qx\n", i+1, dfingers[i].id);
  }
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

// XXX should also check dfingers
bool
Koorde::stabilized (vector<ConsistentHash::CHID> lid)
{
  bool r = Chord::stabilized (lid);

  if (!r) return false;
  r = false;

  printf ("koorde::stabilized? me %qx debruijn %qx d %qx\n", me.id, debruijn, 
	  dfingers[0].id);

  assert (lid.size () > 0);

  if (lid.size () == 1) {
    assert (dfingers[0].id == lid.front ());
    r = true;
  } else {
    uint i;

    for (i = 0; i < lid.size (); i++) {
      uint j;
      // printf ("stable? %qx %qx %qx, %qx\n", lid[i], lid[(i+1)%lid.size ()], 
      //      dfingers[0].id, debruijn);
      if (ConsistentHash::betweenrightincl (lid[i], lid[(i+1) % lid.size ()], 
					    debruijn)) {
	r = true;
	for (j = 0; j < k - 1; j++) {
	  if (lid[i] != dfingers[j].id) {
	    r = false;
	    break;
	  }
	  i = (i + 1) % lid.size ();
	}
	if (!r) {
	  printf ("loop: not stable: finger %d should be %qx but is %qx\n", 
		  j, lid[i], dfingers[j].id);
	}
	r = true;
	break;
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
  printf ("Koorde (%u,%qx) debruijn %qx\n", me.ip, me.id, debruijn);
  for (uint i = 0; i < dfingers.size (); i++) {
    printf ("Koorde %d debruijn finger %qx\n", i+1, dfingers[i].id);
  }
	  
}
