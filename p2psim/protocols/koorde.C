/*
 * Copyright (c) 2003 [NAMES_GO_HERE]
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#include "koorde.h"
#include <stdio.h>

extern bool vis;
extern bool static_sim;

#define INIT 1

Koorde::Koorde(Node *n, Args &a) : Chord(n, a) 
{
  if (a.find("base") != a.end()) {
    logbase = atoi(a["base"].c_str());
  }else{
    logbase = 2;
  }
  if (a.find("resilience") != a.end()) {
    resilience = atoi(a["successors"].c_str());
  }else{
    resilience = 1;
  }

  if (a.find("fingers") != a.end()) {
    fingers = atoi(a["successors"].c_str());
  }else{
    fingers = 16;
  }

  k = 1 << logbase; 
  debruijn = me.id << logbase;
  printf ("Koorde(%u,%qx):debruijn=%qx base %u k %u nsucc %u res %u fing %u\n", 
	  me.ip, me.id, debruijn, k, logbase, _nsucc, resilience, fingers);
  loctable->pin (debruijn, fingers-1, 1);
  isstable = true;
}

// Create an imaginary node i with as many bits from k as possible and
// such that start < i <= succ.
Chord::CHID 
Koorde::firstimagin (CHID start, CHID succ, CHID k, CHID *kr) 
{
  Chord::CHID i = start + 1;

  //  printf ("start %16qx succ %16qx k %16qx\n", start, succ, k);

  if (start == succ) {  // XXX yuck
    *kr = k;
  } else {
    uint bs;
    ConsistentHash::CHID top;
    ConsistentHash::CHID bot;
    ConsistentHash::CHID j;
    for (bs = NBCHID - logbase - 1; bs > 0; bs -= logbase) {
      assert (((NBCHID - 1 - bs) % logbase) == 0);
      top = start >> (bs + 1);
      i = top << (bs + 1);
      j = (top + 1) << (bs + 1);
      bot = k >> (NBCHID - bs - 1);
      i = i | bot;
      j = j | bot;
      if (ConsistentHash::betweenrightincl (start, succ, i)) {
	break;
      }
      if (ConsistentHash::betweenrightincl (start, succ, j)) {
	i = j;
	break;
      }
    }
    if (bs > 0) {
      // shift bs top bits out k
      *kr = k << (bs + 1);
    } else {
      *kr = k;
    }
    // printf ("start %qx succ %qx i %qx k %qx bs %d kbits %d kr %qx\n",
    //      start, succ, i, k, bs, NBCHID - 1 - bs, *kr);
  }
  // printf ("i %16qx kr %16qx\n", i, *kr);
  return i;
}

Chord::CHID 
Koorde::nextimagin (CHID i, CHID kshift)
{
  uint t = ConsistentHash::getbit (kshift, NBCHID - logbase, logbase);
  CHID r = (i << logbase) | t;
  //printf ("nextimagin: kshift %qx topbit is %u, i is %qx new i is %qx\n",
  //kshift, t, i, r);
  return r;
}

// Iterative version of the figure 2 algo in IPTPS'03 paper.
vector<Chord::IDMap>
Koorde::find_successors(CHID key, uint m, bool is_lookup)
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

  if (vis && is_lookup) 
    printf ("vis %llu search %16qx %16qx %16qx\n", now(), me.id, key, r.i);

  while (1) {
    if ((r.i == 0) || (count++ >= 1000)) {
      printf ("find_successor: key = %16qx\n", key);
      for (uint i = 0; i < path.size (); i++) {
	printf ("  %16qx i %16qx k %16qx\n", path[i].id, ipath[i], kpath[i]);
      }
      assert (0);
    }

    a.kshift = r.kshift;
    a.i = r.i;

    last = r.next;

    path.push_back (r.next);
    ipath.push_back (a.i);
    kpath.push_back (a.kshift);

    record_stat(is_lookup?1:0);
    if (!doRPC (r.next.ip, &Koorde::koorde_next, &a, &r)) {
      if (!node()->alive()) {
	r.v.clear();
	break;
      }
      timeout++;
      assert(r.next.ip != me.ip);
      loctable->del_node (path.back ());
      path.pop_back ();
      ipath.pop_back ();
      kpath.pop_back ();

      if (path.size () > 0) {

	alert_args aa;
	alert_ret ar;
	assert(r.next.ip != me.ip);
	aa.n = r.next;

	r.next = path.back ();
	r.kshift = kpath.back ();
	r.i = ipath.back ();

	record_stat(is_lookup?1:0);
        doRPC (r.next.ip, &Chord::alert_handler, &aa, &ar);

	path.pop_back ();
	ipath.pop_back ();
	kpath.pop_back ();

      } else {
	r.v.clear ();
	break; 
      }

    }

    if (vis && is_lookup) 
      printf ("vis %llu step %16qx %16qx %16qx\n", now (), me.id, last.id,
	      r.i);
    
    if (r.done) break;
  }


  if (r.v.size () > 0) {
    path.push_back (r.next);
    ipath.push_back (r.i);
    kpath.push_back (r.kshift);
  }

  if (is_lookup) {
    printf ("find_successor for (id %qx, key %qx):",  me.id, key);
    if (r.v.size () > 0) {
      uint cor = 0;
      uint debruijn = 0;
      assert (path.size () >= 2);
      for (uint i = 0; i < path.size () - 2; i++) {
	if (ipath[i] == ipath[i+1]) cor++;
	else debruijn++;
      }
      assert ((cor + debruijn) == (path.size () - 2));
      printf (" is (%u,%qx) hops %u cor %u debruijn %u cor+debruijn %u timeout %d\n", 
	      r.v[0].ip, 
	      r.v[0].id, path.size () - 1, cor, debruijn, cor + debruijn, 
	      timeout);
      for (uint i = 0; i < path.size (); i++) {
       printf ("  %16qx i %16qx k %16qx\n", path[i].id, ipath[i], kpath[i]);
      }

      CHID s = r.v[0].id;
  //    assert ((me.id == mysucc.id) || (me.id == s) ||
//	      ConsistentHash::betweenrightincl (me.id, s, key));
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
    //printf ("%s Koorde_next: done succ key = %qx: (%u,%qx)\n", 
     //  ts(), a->k, succ.ip, succ.id);
  } else if (a->i == me.id || ConsistentHash::betweenrightincl (me.id, succ.id, a->i)) {
    r->k = a->k;
    r->kshift = a->kshift;
    r->i = a->i;
    do {
      r->i = nextimagin (r->i, r->kshift);
      r->kshift = r->kshift << logbase;
    }while (ConsistentHash::betweenrightincl(me.id,succ.id,r->i));
    r->next = loctable->pred(r->i);
    r->done = false;
    //printf ("%s Koorde_next: contact de bruijn (%u,%qx) i=%qx kshift=%qx debruijn pointer %qx\n", ts(),
     // r->next.ip, r->next.id, r->i, r->kshift, debruijn);
  } else {
    r->k = a->k;
    r->next = loctable->pred (a->i);
    r->kshift = a->kshift;
    r->i = a->i;
    r->done = false;
    assert (ConsistentHash::betweenrightincl (me.id, r->i, r->next.id));
    //printf ("%s Koorde_next: follow succ (%u,%qx) pointer to (%u,%qx) i=%qx kshift=%qx debruijn pointer %qx\n", ts(),
     // succ.ip, succ.id, r->next.ip, r->next.id, r->i, r->kshift, debruijn);

  }
}

void
Koorde::fix_debruijn () 
{

  get_successor_list_args gsa;
  get_successor_list_ret gsr;
  bool ok;

  //try a cheap way to fix debruijn predecessor first
  assert(resilience <= _nsucc);
  IDMap dpred = loctable->pred(debruijnpred);

  if (dpred.ip == me.ip) goto NEXT;
    
  gsa.m = _nsucc;
  record_stat();
  ok = doRPC(dpred.ip, &Chord::get_successor_list_handler, &gsa,&gsr);
  if (!node()->alive()) return;
  if ( ok && gsr.v.size() > 0 && ConsistentHash::between(dpred.id, gsr.v[0].id, debruijnpred)) {
    loctable->add_node(dpred);
    for (uint i = 0; i < gsr.v.size(); i++) 
      loctable->add_node(gsr.v[i]);
  }else {
    if (!ok) loctable->del_node(dpred);
    vector<IDMap> scs = find_successors(debruijnpred, resilience-1, false);
    if (scs.size() > 0) {
      loctable->add_node(last);
      for (uint i = 0; i < scs.size(); i++) {
	loctable->add_node(scs[i]);
      }
    }
  }
NEXT:
  //try a cheap way to test for the validity of debruijn fingers first
  dpred = loctable->pred(debruijn);
  gsa.m = fingers;
  assert(fingers <= _nsucc);
  record_stat();
  ok = doRPC(dpred.ip, &Chord::get_successor_list_handler, &gsa, &gsr);
  if (!node()->alive()) return;
  if (ok && gsr.v.size() > 0 && ConsistentHash::betweenrightincl(dpred.id, gsr.v[0].id, debruijn)) {
    loctable->add_node(dpred);
    for (uint i = 0; i < gsr.v.size(); i++) {
      loctable->add_node(gsr.v[i]);
    }
    //printf("%s stabilize fix_debruijn cheap finished %qx, its succ %d,%qx its last(pred) %d,%qx\n", 
//	ts(), debruijn, gsr.v[0].ip, gsr.v[0].id, dpred.ip, dpred.id);
  } else {
    if (!ok) loctable->del_node(dpred);
    vector<IDMap> scs = find_successors (debruijn, fingers - 1, false);
    if (scs.size() > 0) {
 //     printf("%s stabilize fix_debruijn finished debruijn %qx, its succ %d,%qx its last(pred) %d,%qx\n", 
//	  ts(), debruijn, scs[0].ip, scs[0].id, last.ip, last.id);
      loctable->add_node (last);
      for (uint i = 0; i < scs.size (); i++) 
	loctable->add_node (scs[i]);
    }
  }
  
#if 0
  printf ("fix_debruijn (%u,%qx): debruijn %qx succ %qx d %u,%qx at %lu\n",
	  me.ip, me.id, debruijn, scs[0].id, last.ip, last.id, now ());
  for (uint i = 0; i < scs.size(); i++) {
    printf ("  Koorde %d debruijn fingers %u,%qx\n", i+1, scs[i].ip,
	    scs[i].id);
  }
#endif
  
  if (vis) {
    bool change = false;
    IDMap d = loctable->pred (debruijn);
    vector<IDMap> sc = loctable->succs (d.id + 1, fingers - 1);
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
  if ((!static_sim) || (!isstable)) {
    Koorde::stabilize();
    delaycb(_stabtimer, &Koorde::reschedule_stabilizer, (void *)0);
  }
}

void
Koorde::stabilize()
{
  //printf ("%s Koorde stabilize Chord start\n",ts());
  Chord::stabilize();
  //printf ("%s Koorde stabilize Chord finish, stabilize debruijn start %qx\n",ts(),debruijn);
  fix_debruijn();
  printf ("%s Koorde stabilize debruijn finish %qx\n",ts(),debruijn);
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
    //   printf ("succ finger %d is %qx\n", i, sc[i].id);
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
  r = debruijn_stabilized (debruijn, fingers, lid);
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
  debruijn_dump (debruijn, fingers);
  if (resilience > 0) debruijn_dump (debruijnpred, resilience);
}


