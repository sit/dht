#include "koorde.h"
#include <stdio.h>
#include <iostream>
using namespace std;

extern bool vis;

#define INIT 1

Koorde::Koorde(Node *n, uint base, uint ns, uint r) : 
  Chord(n, ns)
{
  logbase = base;
  k = 1 << logbase; 
  resilience = r;
  nsucc = ns;
  debruijn = me.id << logbase;
  printf ("Koorde: (%u,%qx) debruijn=%qx base %u k %u nsucc %u res %u\n", 
	  me.ip, me.id, debruijn, k, base, nsucc, resilience);
  loctable->pin (debruijn, nsucc, 1);
  isstable = true;
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
  int timeout = 0;
  koorde_lookup_arg a;
  koorde_lookup_ret r;
  vector<IDMap> path;
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
    printf ("vis %llu search %16qx %16qx %16qx\n", now(), me.id, key, r.i);

  while (1) {
    assert (count++ < 5000);

    a.kshift = r.kshift;
    a.i = r.i;

    // printf("nexthop (%u,%qx) is (%u,%qx) key %qx i=%qx, kshift %qx)\n", 
    //   me.ip, me.id, r.next.ip, r.next.id, a.k, r.i, r.kshift);

    last = r.next;

    path.push_back (r.next);
    ipath.push_back (a.i);
    kpath.push_back (a.kshift);

    if (!doRPC (r.next.ip, &Koorde::koorde_next, &a, &r)) {
      timeout++;
      printf ("rpc failure %16qx to %16qx at %llu\n", me.id, r.next.id,
	      now ());
      loctable->del_node (path.back ());
      path.pop_back ();
      ipath.pop_back ();
      kpath.pop_back ();
      if (path.size () > 0) {
	alert_args aa;
	alert_ret ar;
	r.next = path.back ();
	r.kshift = kpath.back ();
	r.i = ipath.back ();
        doRPC (r.next.ip, &Chord::alert_handler, &aa, &ar);
      } else {
	r.v.clear ();
	break; 
      }

    }

    if (vis && !intern) 
      printf ("vis %llu step %16qx %16qx %16qx\n", now (), me.id, last.id,
	      r.i);
    
    if (r.done) break;
  }


  if (r.v.size () > 0) {
    path.push_back (r.next);
    ipath.push_back (r.i);
    kpath.push_back (r.kshift);
  }

  if (!intern) {
    printf ("find_successor for (id %qx, key %qx):",  me.id, key);
    if (r.v.size () > 0) {
      int cor = 0;
      for (uint i = 0; i < path.size () - 1; i++) {
	if (ipath[i] == ipath[i+1]) cor++;
      }
      printf (" is (%u,%qx) hops %d cor %d debruijn %d timeout %d\n", 
	      r.v[0].ip, 
	      r.v[0].id, path.size (), cor, path.size () - cor, timeout);
      for (uint i = 0; i < path.size (); i++) {
	printf ("  %16qx i %16qx k %16qx\n", path[i].id, ipath[i], kpath[i]);
      }
      CHID s = r.v[0].id;
      assert ((me.id == mysucc.id) || (me.id == s) ||
	      ConsistentHash::betweenrightincl (me.id, s, key));
    } else {
      printf (" failed\n");
    }
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
      printf ( "vis %llu dfingers %16qx %16qx", now (), me.id, debruijn);
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
  if (!isstable) {
    Koorde::stabilize();
    delaycb(STABLE_TIMER, &Koorde::reschedule_stabilizer, (void *)0);
  }
}

void
Koorde::stabilize()
{
  printf ("stabilize %qx %llu\n", me.id, now ());
  Chord::stabilize();
  fix_debruijn();
}

bool
Koorde::debruijn_stabilized (ConsistentHash::CHID finger, uint n,
			     vector<ConsistentHash::CHID> lid)
{
  IDMap d = loctable->pred (finger);
  vector<IDMap> sc = loctable->succs (d.id + 1, n - 1);
  vector<IDMap> dfingers;

  dfingers.clear ();
  dfingers.push_back (d);
  for (uint i = 0; i < sc.size (); i++) {
    dfingers.push_back (sc[i]);
    // printf ("succ finger %d is %qx\n", i, sc[i].id);
  }
  assert (dfingers.size () <= n);

  bool r = false;
  // printf ("koorde::stabilized? %u,%qx debruijn %qx d %qx at %lu %d\n", me.ip,
  //  me.id, finger, dfingers[0].id, now (), isstable);

  assert (lid.size () > 0);

  if (lid.size () == 1) {
    assert (d.id == lid.front ());
    r = true;
  } else {
    uint i, j;

    for (i = 0; i < lid.size (); i++) {
      // printf ("stable? %qx %qx %qx, %qx\n", lid[i], lid[(i+1)%lid.size ()], 
      //      dfingers[0].id, finger);
      if (ConsistentHash::betweenrightincl (lid[i], lid[(i+1) % lid.size ()], 
					    finger)) {
	break;
      }
    }

    for (j = 0; j < n; j++) {
      if (lid[i] != dfingers[j].id) {
	break;
      }
      i = (i + 1) % lid.size ();
    }

    if (j == n) {
      r = true;
    } else {
      printf ("loop %u,%qx not stable: finger %d should be %qx but is %qx\n", 
	      me.ip, me.id, j, lid[i], dfingers[j].id);
      for (uint l = 0; l < dfingers.size (); l++) {
	printf ("succ debruijn finger %d is %qx\n", l, dfingers[l].id);
      }

    }
  }
  return r;
}

bool
Koorde::stabilized (vector<ConsistentHash::CHID> lid)
{
  bool r = Chord::stabilized (lid);
  if (!r) return false;
  r = debruijn_stabilized (debruijn, k, lid);
  if (r && (resilience > 0)) 
    r = debruijn_stabilized (debruijnpred, resilience, lid);
  return r;
}

void 
Koorde::init_state(vector<IDMap> ids)
{
  uint nnodes = ids.size ();

  Chord::CHID y = ConsistentHash::log_b ((Chord::CHID) nnodes, 2);
  Chord::CHID x = (Chord::CHID) 1;
  x = (x << (NBCHID-1));  // XXX C++ compiler bug; want to shift NBCHID
  x = (x << 1) - 1;
  x = (x / nnodes);
  x = x * y;
  debruijnpred = debruijn - x;
  printf ("debruinpred is %16qx and x is %qx\n", debruijnpred, x);

  if (resilience > 0) {
    loctable->pin (debruijnpred, resilience, 1);
  }

  Chord::init_state (ids);
}

void
Koorde::debruijn_dump (Chord::CHID finger, uint n)
{
  IDMap d = loctable->pred (finger);
  vector<IDMap> sc = loctable->succs (d.id + 1, n - 1);
  vector<IDMap> dfingers;

  dfingers.clear ();
  dfingers.push_back (d);
  for (uint i = 0; i < sc.size (); i++) {
    dfingers.push_back (sc[i]);
    // printf ("succ finger %d is %qx\n", i, sc[i].id);
  }
  assert (dfingers.size () <= n);

  printf ("Koorde (%5u,%16qx) finger %16qx\n", me.ip, me.id, finger);
  for (uint m = 0; m < dfingers.size (); m++) {
    printf ("finger %3u is %16qx\n", m, dfingers[m].id);
  }
}

void
Koorde::dump ()
{
  isstable = true;

  Chord::dump ();
  debruijn_dump (debruijn, k);
  if (resilience > 0) debruijn_dump (debruijnpred, resilience);
}


