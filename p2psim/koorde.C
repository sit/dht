#include "koorde.h"
#include <stdio.h>
#include <iostream>
using namespace std;

extern bool vis;

Koorde::Koorde(Node *n) : Chord(n, k) 
{
  debruijn = me.id << logbase;
  printf ("Koorde: (%u,%qx) %d debruijn=%qx\n", me.ip, me.id, k, debruijn);
  loctable->pin (debruijn, k, 1);
}

  
// Create an imaginary node i with as many bits from k as possible and
// such that start < i <= succ.
Chord::CHID 
Koorde::firstimagin (CHID start, CHID succ, CHID k, CHID *kr) 
{
  Chord::CHID i;

  if (start == succ) {  // XXX yuck
    i = start + 1;
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
      //      printf ("start %qx succ %qx i %qx k %qx bs %d kr %qx\n",
      //           start, succ, i, k, bs, *kr);
    } else {
      *kr = k;
    }
    assert (ConsistentHash::betweenrightincl (start, succ, i));
  }
  return i;
}

Chord::CHID 
Koorde::nextimagin (CHID i, CHID kshift)
{
  uint t = ConsistentHash::getbit (kshift, NBCHID - logbase, logbase);
  CHID r = (i << logbase) | t;
  // printf ("nextimagin: kshift %qx topbit is %u, i is %qx new i is %qx\n",
  //  kshift, t, i, r);
  return r;
}

// Iterative version of the figure 2 algo in IPTPS'03 paper.
vector<Chord::IDMap>
Koorde::find_successors(CHID key, uint m, bool intern)
{
  int count = 0;
  koorde_lookup_arg a;
  koorde_lookup_ret r;
  vector<ConsistentHash::CHID> path;
  vector<ConsistentHash::CHID> ipath;
  vector<ConsistentHash::CHID> kpath;
  IDMap mysucc = loctable->succ(me.id + 1);

  a.nsucc = m;
  a.k = key;
  r.next = me;
  r.k = key;
  r.i = firstimagin (me.id, mysucc.id, a.k, &r.kshift);

  // printf ("find_successor(ip %u,id %qx, key %qx, i=%qx)\n", me.ip, me.id, 
  //  a.k,  r.i);

  if (vis && !intern) 
    printf ("vis %lu search %16qx %16qx\n", now(), me.id, key);
  while (1) {
    assert (count++ < 1000);

    a.kshift = r.kshift;
    a.i = r.i;

    // printf("nexthop (%u,%qx) is (%u,%qx) key %qx i=%qx, kshift %qx)\n", 
    //   me.ip, me.id, r.next.ip, r.next.id, a.k, r.i, r.kshift);

    last = r.next;

    path.push_back (r.next.id);
    ipath.push_back (a.i);
    kpath.push_back (a.kshift);

    if (vis && !intern) 
      printf ("vis %lu step %16qx %16qx\n", now (), me.id, r.next.id);

    doRPC (r.next.ip, &Koorde::koorde_next, &a, &r);
    
    if (r.done) break;
  }

  assert (r.v.size () > 0);

  if (vis & !intern) 
    printf ("vis %lu step %16qx %16qx\n", now (), me.id, r.v[0].id);

  path.push_back (r.next.id);
  ipath.push_back (r.i);
  kpath.push_back (r.kshift);

  if (!intern) {
    printf ("find_successor for (id %qx, key %qx) is (%u,%qx) hops %d\n", 
	    me.id, key, r.v[0].ip, r.v[0].id, count);
    for (uint i = 0; i < path.size () - 1; i++) {
      printf ("  %16qx i %16qx k %16qx\n", path[i], ipath[i], kpath[i]);
    }
    CHID s = r.v[0].id;
    assert ((me.id == mysucc.id) || (me.id == s) ||
	  ConsistentHash::betweenrightincl (me.id, s, key));
  }

  return r.v;
}

void
Koorde::koorde_next(koorde_lookup_arg *a, koorde_lookup_ret *r)
{
  IDMap succ = loctable->succ(me.id + 1);
  //printf ("Koorde_next (id=%qx, key=%qx, kshift=%qx, i=%qx) succ=(%u,%qx)\n", 
  //me.id, a->k, a->kshift, a->i, succ.ip, succ.id);
  if (ConsistentHash::betweenrightincl (me.id, succ.id, a->k) ||
      (me.id == succ.id)) {
    r->next = succ;
    r->k = a->k;
    r->kshift = a->kshift;
    r->i = a->i;
    r->done = true;
    r->v.clear ();
    r->v = loctable->succs(me.id + 1, a->nsucc);
    assert (r->v.size () > 0);
    //    printf ("Koorde_next: done succ key = %qx: (%u,%qx)\n", 
    //	    a->k, succ.ip, succ.id);
  } else if (ConsistentHash::betweenrightincl (me.id, succ.id, a->i)) {
    r->k = a->k;
    r->kshift = a->kshift << logbase;
    r->i = nextimagin (a->i, a->kshift);
    r->next = loctable->pred(r->i);
    r->done = false;
    //printf ("Koorde_next: contact de bruijn (%u,%qx) i=%qx kshift=%qx\n", 
    //  r->next.ip, r->next.id, r->i, r->kshift);
  } else {
    r->k = a->k;
    r->next = loctable->pred (a->i);
    r->kshift = a->kshift;
    r->i = a->i;
    r->done = false;
    assert (ConsistentHash::betweenrightincl (me.id, r->i, r->next.id));
  }
}

void
Koorde::fix_debruijn () 
{
  //  printf ("fix_debruijn %u\n", isstable);
  vector<IDMap> scs = find_successors (debruijn, k - 1, true);
  assert (scs.size () > 0);
  loctable->add_node (last);
  for (uint i = 0; i < scs.size (); i++) {
    loctable->add_node (scs[i]);
  }
#if 0
  printf ("fix_debruijn (%u,%qx): debruijn %qx succ %qx d %u,%qx at %lu\n",
	  me.ip, me.id, debruijn, scs[0].id, last.ip, last.id, now ());
  for (uint i = 0; i < scs.size(); i++) {
    printf ("  Koorde %d debruijn fingers %u,%qx\n", i+1, scs[i].ip,
	    scs[i].id);
  }
#endif
  assert (scs.size () <= k);
  
  if (vis) {
    bool change = false;
    IDMap d = loctable->pred (debruijn);
    vector<IDMap> sc = loctable->succs (d.id + 1, k - 1);
    vector<IDMap> dfingers;

    dfingers.push_back (d);
    for (uint i = 0; i < sc.size (); i++) {
      dfingers.push_back (sc[i]);
    }

    for (uint i = 0; i < dfingers.size (); i++) {
      if ((i >= lastdfingers.size ()) || lastdfingers[i].id != dfingers[i].id) {
	change = true;
      }
    }

    if (change) {
      printf ( "vis %lu dfingers %16qx %16qx", now (), me.id, debruijn);
      for (uint i = 0; i < dfingers.size (); i++) {
	printf ( " %16qx", dfingers[i].id);
      }
      printf ( "\n");
    }
    lastdfingers = dfingers;
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
  if (!isstable) {
    printf ("stabilize %qx %lu\n", me.id, now ());
    Chord::stabilize();
    fix_debruijn();
  }
}

bool
Koorde::stabilized (vector<ConsistentHash::CHID> lid)
{
  bool r = Chord::stabilized (lid);
  if (!r) return false;

  IDMap d = loctable->pred (debruijn);
  vector<IDMap> sc = loctable->succs (d.id + 1, k - 1);
  vector<IDMap> dfingers;

  dfingers.clear ();
  dfingers.push_back (d);
  for (uint i = 0; i < sc.size (); i++) {
    dfingers.push_back (sc[i]);
    // printf ("succ finger %d is %qx\n", i, sc[i].id);
  }
  assert (dfingers.size () <= k);

  r = false;
  // printf ("koorde::stabilized? %u,%qx debruijn %qx d %qx at %lu %d\n", me.ip,
  //  me.id, debruijn, dfingers[0].id, now (), isstable);

  assert (lid.size () > 0);

  if (lid.size () == 1) {
    assert (d.id == lid.front ());
    r = true;
  } else {
    uint i, j;

    for (i = 0; i < lid.size (); i++) {
      // printf ("stable? %qx %qx %qx, %qx\n", lid[i], lid[(i+1)%lid.size ()], 
      //      dfingers[0].id, debruijn);
      if (ConsistentHash::betweenrightincl (lid[i], lid[(i+1) % lid.size ()], 
					    debruijn)) {
	break;
      }
    }

    for (j = 0; j < k; j++) {
      if (lid[i] != dfingers[j].id) {
	break;
      }
      i = (i + 1) % lid.size ();
    }

    if (j == k) {
      r = true;
    } else {
      printf ("loop %u,%qx not stable: finger %d should be %qx but is %qx\n", 
	      me.ip, me.id, j, lid[i], dfingers[j].id);
      for (uint l = 0; l < dfingers.size (); l++) {
	printf ("succ finger %d is %qx\n", l, dfingers[l].id);
      }

    }
  }
  return r;
}

void
Koorde::dump ()
{
  isstable = true;

  Chord::dump ();

  IDMap d = loctable->pred (debruijn);
  vector<IDMap> sc = loctable->succs (d.id + 1, k - 1);
  vector<IDMap> dfingers;

  dfingers.clear ();
  dfingers.push_back (d);
  for (uint i = 0; i < sc.size (); i++) {
    dfingers.push_back (sc[i]);
    // printf ("succ finger %d is %qx\n", i, sc[i].id);
  }
  assert (dfingers.size () <= k);

  printf ("Koorde (%5u,%16qx) debruijn %16qx\n", me.ip, me.id, debruijn);
  for (uint m = 0; m < dfingers.size (); m++) {
    printf ("finger %3u is %16qx\n", m, dfingers[m].id);
  }

}
