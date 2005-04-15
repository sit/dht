/*
 * Copyright (c) 2003-2005 [Frans Kaashoek]
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
#include "observers/chordobserver.h"
#include <stdio.h>
#include <iostream>

using namespace std;

extern bool vis;
extern bool static_sim;

#define INIT 1

Koorde::Koorde(IPAddress i, Args &a) : Chord(i, a) 
{
  logbase = a.nget<uint>("logbase",1,10);
  resilience = a.nget<uint>("successors", 16, 10);
  fingers = a.nget<uint>("fingers", 16, 10);
  _stab_debruijn_timer = a.nget<uint>("debruijntimer",10000,10); 

  k = 1 << logbase; 
  debruijn = me.id << logbase;
  _max_lookup_time = a.nget<uint>("maxlookuptime",20000,10);
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

void
Koorde::join(Args *args)
{
  IDMap wkn;
  if (args) {
    wkn.ip = args->nget<IPAddress>("wellknown");
    assert (wkn.ip);
    wkn.id = ConsistentHash::ip2chid(wkn.ip);
  }else{
    wkn = _wkn;
  }
  find_successors_args fa;
  find_successors_ret fr;

  CDEBUG(3) << "start to join" << endl;
  Chord::join(args);
  if (!alive()) return;
  debruijn = me.id << logbase;
  IDMap succ = loctable->succ(me.id+1);
  debruijnpred = debruijn - (CHID)(succ.id-me.id)*resilience;

  fa.key = debruijn;
  fa.m = fingers-1;
  record_stat(me.ip,_wkn.ip,TYPE_JOIN_LOOKUP,1,0);
  bool ok = doRPC(wkn.ip, &Chord::find_successors_handler, &fa, &fr);
  if (!alive()) return;
  assert(ok);
  record_stat(_wkn.ip,me.ip,TYPE_JOIN_LOOKUP,fr.v.size(),0);
  if (fr.v.size() > 0) {
    loctable->add_node(fr.v[0]);
    if (fr.last.ip > 0) loctable->add_node(fr.last);
  }
  //schedule finger stabilizer
  if (!_stab_debruijn_running) {
    _stab_debruijn_running = true;
    reschedule_debruijn_stabilizer(NULL); //a hack, no null means restart fixing fingres
  }else{
    fix_debruijn();
  }
}

// Iterative version of the figure 2 algo in IPTPS'03 paper.
vector<Chord::IDMap>
Koorde::find_successors(CHID key, uint m, uint type, IDMap *lasthop, lookup_args *args)
{
  int timeout = 0;
  int hops = 0;
  Time time_timeout = 0;
  koorde_lookup_arg a;
  koorde_lookup_ret r;
  vector<IDMap> path;
  vector<ConsistentHash::CHID> ipath;
  vector<ConsistentHash::CHID> kpath;
  Time before = now();
  if (!_inited) 
    return r.v;
  IDMap mysucc = loctable->succ(me.id + 1);

  a.nsucc = m;
  a.k = key;
  a.dead.ip = 0;
  r.next = me;
  r.k = key;
  r.i = firstimagin (me.id, mysucc.id, a.k, &r.kshift);

  CDEBUG(2) << "find_successors key " << printID(a.k) << "i="
    << printID(r.i) << endl;

  if (vis && type == TYPE_USER_LOOKUP) 
    printf ("vis %llu search %16qx %16qx %16qx\n", now(), me.id, key, r.i);

  while (1) {
    if ((r.i == 0) || (path.size() >= 30)) {
      CDEBUG(0) << "find_successors key " << printID(key) << endl;
      for (uint i = 0; i < path.size (); i++) {
	CDEBUG(0)<< path[i].ip << "," << printID(path[i].id) << printID(ipath[i])<<printID(kpath[i])<<endl;
      }
      assert(0);
    }

    a.kshift = r.kshift;
    a.i = r.i;

    if (lasthop) {
      *lasthop = r.next;
    }

    path.push_back (r.next);
    ipath.push_back (a.i);
    kpath.push_back (a.kshift);

    Time t_out = TIMEOUT(me.ip,r.next.ip);
    if (r.next.ip!=me.ip) 
      hops++;
    
    IDMap nextnode = r.next;
    if (nextnode.ip!=me.ip)
      record_stat(me.ip,nextnode.ip,type,3,0);


    CDEBUG(3) << "find_successors key " << printID(a.k) << "begin to contact " 
      <<  r.next.ip << " interval " << (now()-before) << endl;
    bool ok = doRPC(r.next.ip, &Koorde::koorde_next, &a, &r, t_out);
    CDEBUG(3) << "find_successors key " << printID(a.k) << " contacted next "
      << nextnode.ip << "," << printID(nextnode.id) << "ok? " << (ok?1:0) 
      << " done? " << (r.done?1:0) << " next " << r.next.ip << " t_out " 
      << t_out << endl;

    
    if (args && args->latency >= _max_lookup_time) 
      break;

    if ((!ok) || (!r.next.ip)) {
      if (!alive()) {
	r.v.clear();
	break;
      }
      if (!ok) {
	time_timeout += t_out;
	timeout++;
      }
      a.dead = path.back();

      path.pop_back ();
      ipath.pop_back ();
      kpath.pop_back ();

      if (path.size () > 0) {

	/*
	alert_args aa;
	assert(r.next.ip != me.ip);
	aa.n = r.next;
	*/

	r.next = path.back ();
	r.kshift = kpath.back ();
	r.i = ipath.back ();

	/*
	if (!ok) {
	  record_stat(type,1,0);
	  CDEBUG(3) << "find_successors key " << printID(a.k) << " alert previous "
	    << r.next.ip << "," << printID(r.next.id) << endl;
	  doRPC (r.next.ip, &Chord::alert_handler, &aa, (void *)NULL);
	  CDEBUG(3) << "find_successors key " << printID(a.k) << " alert done" << endl;
	  record_stat(type,0,0);
	}
	*/
	path.pop_back ();
	ipath.pop_back ();
	kpath.pop_back ();

      } else {
	r.v.clear ();
	break; 
      }

    }else {
      a.dead.ip = 0;
    }

    if (nextnode.ip!=me.ip)
      record_stat(nextnode.ip,me.ip,type,1,0);

    if (vis && type == TYPE_USER_LOOKUP) 
      printf ("vis %llu step %16qx %16qx %16qx\n", now (), me.id, lasthop->id,
	      r.i);
    
    if (r.done) break;
  }


  if (r.v.size () > 0) {
    path.push_back (r.next);
    ipath.push_back (r.i);
    kpath.push_back (r.kshift);
  }

  if (type == TYPE_USER_LOOKUP) {

    if (!check_correctness(key,r.v)) {
      r.v.clear();
    }
    CDEBUG(3) << " done lookup up key " << printID(key) << 
      " before " << before << " interval " << (now()-before) 
      << " hops " << hops << " beforelat " << args->latency << endl;
    if (args) {
      args->latency += (now()-before);
      args->num_to += timeout;
      args->total_to += time_timeout;
      args->hops += hops;
    }
  }

  return r.v;
}

void
Koorde::koorde_next(koorde_lookup_arg *a, koorde_lookup_ret *r)
{

  //i have not joined successfully, refuse to answer query
  IDMap succ = loctable->succ(me.id+1);
  if (!succ.ip) {
    CDEBUG(1) << " not yet stabilized refuse to reply to key " 
      << printID(a->k) << endl;
    r->next.ip = 0;
    r->next.id = 0;
    r->done = false;
    if (!_join_scheduled) {
      _join_scheduled++;
      delaycb(0, &Koorde::join, (Args *)0);
    }
    return;
  }

  //mark all the bad nodes
  if (a->dead.ip)
    loctable->add_check(a->dead);

  //printf ("koorde_next (id=%qx, key=%qx, kshift=%qx, i=%qx) succ=(%u,%qx)\n", 
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
    CDEBUG(3) << "koorde_next key " << printID(a->k) << "done: succ " << succ.ip 
      << "," << printID(succ.id) << endl;
  } else if (a->i == me.id || ConsistentHash::betweenrightincl (me.id, succ.id, a->i)) {
    r->k = a->k;
    r->kshift = a->kshift;
    r->i = a->i;
    do {
      r->i = nextimagin (r->i, r->kshift);
      r->kshift = r->kshift << logbase;
    }while (ConsistentHash::betweenrightincl(me.id,succ.id,r->i));
    r->next.id = r->i + 1;
    r->next = loctable->pred(r->i, LOC_HEALTHY);
    r->done = false;
    CDEBUG(3) << "koorde_next key " << printID(a->k) << ": follow debruijn " 
      << r->next.ip << "," << printID(r->next.id) << "i=" << printID(r->i) 
      << "kshift=" << printID(r->kshift) << "debruijn " << printID(debruijn) 
      << endl;
  } else {
    r->k = a->k;
    r->i = a->i;
    r->next.id = r->i + 1;
    r->next = loctable->pred(r->i, LOC_HEALTHY);
    r->kshift = a->kshift;
    r->done = false;
 //   assert (ConsistentHash::betweenrightincl (me.id, r->i, r->next.id));
    CDEBUG(3) << "koorde_next key " << printID(a->k) << "follow succ "<<succ.ip 
      << "," << printID(succ.id) << " to " << r->next.ip << "," 
      << printID(r->next.id) << "i=" << printID(r->i) << "kshift=" 
      << printID(r->kshift) << " debruijn " << printID(debruijn) << endl;
  }
}

void
Koorde::fix_debruijn () 
{

  get_predsucc_args gsa;
  get_predsucc_ret gsr;
  bool ok = false;
  IDMap last;

  //try a cheap way to fix debruijn predecessor first
  assert(resilience <= _nsucc);
  IDMap dpred = loctable->pred(debruijnpred);

  if ((dpred.ip == 0) && (ConsistentHash::between(debruijnpred,debruijn, me.id))) 
    goto NEXT;
    
  gsa.m = _nsucc;
  gsa.pred = true;
  //record_stat();
  if (dpred.ip)
    ok = failure_detect(dpred, &Chord::get_predsucc_handler, &gsa,&gsr, TYPE_FINGER_UP,1,0);
  CDEBUG(3) << "fix_debruijn debruijnpred " << printID(debruijnpred) << dpred.ip << "," 
    << printID(dpred.id) << "ok? " << (ok?1:0) << endl;
  if (!alive()) return;
  if (ok) { //&& gsr.v.size() > 0 && ConsistentHash::between(dpred.id, gsr.v[0].id, debruijnpred)) {
    record_stat(dpred.ip,me.ip,TYPE_FINGER_UP,1+gsr.v.size(),0);
    loctable->add_node(dpred);
    loctable->add_node(gsr.n);
    for (uint i = 0; i < gsr.v.size(); i++) 
      loctable->add_node(gsr.v[i]);
  }else {
    if ((!ok) && dpred.ip)
      loctable->del_node(dpred);
    vector<IDMap> scs = find_successors(debruijnpred, resilience-1, TYPE_FINGER_LOOKUP, &last, NULL);
    if (scs.size() > 0) {
      loctable->add_node(last);
      for (uint i = 0; i < scs.size(); i++) {
	loctable->add_node(scs[i]);
      }
    }
  }
NEXT:
  if (!alive()) return;
  //try a cheap way to test for the validity of debruijn fingers first
  dpred = loctable->pred(debruijn);
  if (!dpred.ip) return;
  gsa.m = fingers;
  gsa.pred = true;
  assert(fingers <= _nsucc);

  ok = failure_detect(dpred, &Chord::get_predsucc_handler, &gsa, &gsr, TYPE_FINGER_LOOKUP,1,0); 
  CDEBUG(3) << "fix_debruijn debruijn " << printID(debruijn) << dpred.ip << "," 
    << printID(dpred.id) << "ok? " << (ok?1:0) << endl;
  if (!alive()) return;
  if (ok && gsr.v.size() > 0 && 
      ConsistentHash::betweenrightincl(dpred.id, gsr.v[gsr.v.size()-1].id, debruijn)) {
    record_stat(dpred.ip,me.ip,TYPE_FINGER_UP,1+gsr.v.size(),0);
    loctable->add_node(dpred);
    loctable->add_node(gsr.n);
    for (uint i = 0; i < gsr.v.size(); i++) {
      loctable->add_node(gsr.v[i]);
    }
    //printf("%s stabilize fix_debruijn cheap finished %qx, its succ %d,%qx its last(pred) %d,%qx\n", 
//	ts(), debruijn, gsr.v[0].ip, gsr.v[0].id, dpred.ip, dpred.id);
  } else {
    if (!ok) loctable->del_node(dpred);
    vector<IDMap> scs = find_successors (debruijn, fingers - 1, TYPE_FINGER_LOOKUP, &last, NULL);
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
Koorde::reschedule_debruijn_stabilizer(void *x)
{
  assert(!static_sim);
  if (!alive()) {
    _stab_debruijn_running = false;
    return;
  }
  _stab_debruijn_running = true;
  if (_stab_debruijn_outstanding > 0) {
  }else{
    _stab_debruijn_outstanding++;
    if (_inited)
      fix_debruijn();
    _stab_debruijn_outstanding--;
    assert(_stab_debruijn_outstanding == 0);
  }
  delaycb(_stab_debruijn_timer, &Koorde::reschedule_debruijn_stabilizer, (void *) 0);
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
Koorde::initstate()
{
  vector<IDMap> ids = ChordObserver::Instance(NULL)->get_sorted_nodes();
  uint nnodes = ids.size ();
  IDMap tmp;
  tmp.id = debruijn;
  uint pos = upper_bound(ids.begin(), ids.end(), tmp, Chord::IDMap::cmp) - ids.begin();
  for (uint i = 0; i < resilience; i++) 
    loctable->add_node(ids[(pos-i)%nnodes], false, true);
  debruijnpred = debruijn - ((Chord::CHID)-1/nnodes)*resilience;
  tmp.id = debruijn;
  pos = upper_bound(ids.begin(), ids.end(), tmp, Chord::IDMap::cmp) - ids.begin();

  CDEBUG(3) << "koorde_init_state sz " << loctable->size() << " debruijn "
    << printID(debruijn) << ids[pos%nnodes].ip << ","
    << printID(ids[pos%nnodes].id) << endl;

  for (uint i = 0; i < fingers; i++) 
    loctable->add_node(ids[(i+pos)%nnodes],false,true);

  Chord::initstate();
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


