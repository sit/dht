/*
 * Copyright (c) 2003 [Jinyang Li]
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

#include  "chordfingerpns.h"
#include "observers/chordobserver.h"
#include "p2psim/network.h"
#include <stdio.h>
extern bool static_sim;

ChordFingerPNS::ChordFingerPNS(IPAddress i, Args& a, LocTable *l, const char *name) 
  : Chord(i, a, New LocTable(), name) 
{ 
  _base = a.nget<uint>("base",2,10);
  _samples = a.nget<int>("samples",_nsucc,10);
  _fingerlets = a.nget<uint>("fingerlets",1,10);

  _stab_pns_running = false;
  _stab_pns_outstanding = 0;

  _stab_pns_timer = a.nget<uint>("pnstimer",_stab_succlist_timer,10);

  learntable = New LocTable();
  learntable->set_timeout(10*_stab_pns_timer);
  learntable->set_evict(false);
  learntable->init(me);
}

void
ChordFingerPNS::oracle_node_died(IDMap n)
{
  Chord::oracle_node_died(n);

  IDMap tmp = loctable->succ(n.id,LOC_ONCHECK);
  if (tmp.ip != n.ip) return;

  //lost one my of finger?
  vector<IDMap> ids = ChordObserver::Instance(NULL)->get_sorted_nodes();
  uint sz = ids.size();
  CHID lap = (CHID) -1;
  while (lap > 1) {
    lap = lap / _base;
    for (uint j = 1; j <= (_base-1); j++) {
      if (ConsistentHash::between(me.id+lap*j, me.id+lap*(j+1), n.id)) {
	loctable->del_node(n);
	//find a replacement for this finger
	IDMap tmpf;
	tmpf.id = lap * j + me.id;
	uint s_pos = upper_bound(ids.begin(), ids.end(), tmpf, Chord::IDMap::cmp) - ids.begin();
	s_pos = s_pos % sz;
	tmpf.id = lap * (j+1) + me.id;
	uint e_pos = upper_bound(ids.begin(), ids.end(), tmpf, Chord::IDMap::cmp) - ids.begin();
	e_pos = e_pos % sz;
	if (s_pos == e_pos) return;

	IDMap min_f = ids[s_pos];
	Topology *t = Network::Instance()->gettopology();
	uint min_l = t->latency(me.ip,min_f.ip);
	int k = 0;
	for (uint i = s_pos; i!= e_pos; i = (i+1)%sz) {
	  if (t->latency(me.ip,ids[i].ip) < min_l) {
	    min_f = ids[i];
	    min_l = t->latency(me.ip, ids[i].ip);
	    k++;
	    if (_samples > 0 && k>= _samples) break;
	  }
	}
	loctable->add_node(min_f);//add replacement pns finger
#ifdef CHORD_DEBUG
	printf("%s oracle_node_died finger del %u,%qx add %u,%qx\n",ts(),n.ip,n.id,min_f.ip,min_f.id);
#endif
	return;
      }
    }
  }
  assert(0);
}

void
ChordFingerPNS::oracle_node_joined(IDMap n)
{
  Chord::oracle_node_joined(n);

  IDMap tmp = loctable->succ(n.id,LOC_ONCHECK);
  if (tmp.ip == n.ip) return;

  //is the new node one of my finger?
  vector<IDMap> ids = ChordObserver::Instance(NULL)->get_sorted_nodes();
  uint sz = ids.size();
  CHID lap = (CHID) -1;
  while (lap > 1) {
    lap = lap / _base;
    for (uint j = 1; j <= (_base-1); j++) {
      if (ConsistentHash::between(me.id+lap*j, me.id+lap*(j+1), n.id)) {
	IDMap s = loctable->succ(me.id + lap * j,LOC_ONCHECK);
	if (!ConsistentHash::between(me.id+lap*j, me.id+lap*(j+1), n.id)) {
	  loctable->add_node(n);
#ifdef CHORD_DEBUG
	  printf("%s oracle_node_joined finger add %u,%qx\n",ts(),n.ip,n.id);
#endif
	  return;
	}
	IDMap tmpf;
	tmpf.id = lap * j + me.id;
	uint s_pos = upper_bound(ids.begin(), ids.end(), tmpf, Chord::IDMap::cmp) - ids.begin();
	uint n_pos = find(ids.begin(),ids.end(),n) - ids.begin();
	n_pos = n_pos + sz;
	if (_samples < 0 || (int)(n_pos - s_pos) <= _samples) {
	  //choose the closer one
	  Topology *t = Network::Instance()->gettopology();
	  if (t->latency(me.ip, n.ip) < t->latency(me.ip, s.ip)) {
	    loctable->del_node(s);
	    loctable->add_node(n);
#ifdef CHORD_DEBUG
	    printf("%s oracle_node_joined finger del %u,%qx add %u,%qx\n",ts(), s.ip,s.id,n.ip,n.id);
#endif
	  }
	} 
	return;
      }
    }
  }
  assert(0);
}

inline static 
void which_finger(uint base, ConsistentHash::CHID id, ConsistentHash::CHID meid, 
    ConsistentHash::CHID &fs, ConsistentHash::CHID &fe)
{
  ConsistentHash::CHID lap = (ConsistentHash::CHID) -1;
  ConsistentHash::CHID finger;
  while (1) {
    lap = lap/base;
    for (uint j = (base-1); j >= 1; j--) {
      finger = lap * j + meid;
      if (ConsistentHash::between(finger,finger+lap,id)) {
	fs = finger;
	fe = finger + lap;
	return;
      }
    }
  }
}

void
ChordFingerPNS::learn_info(IDMap n)
{
  assert(learntable);

  if(loctable->update_ifexists(n)) 
    return;

  if (learntable->update_ifexists(n))
    return;

  Topology *t = Network::Instance()->gettopology();
  IDMap ns = loctable->succ(n.id+1,LOC_ONCHECK);
  LocTable::idmapwrap *naked_ns = NULL;

  if ((ns.ip==0) || (ns.ip == me.ip))
    goto NEXT_CHECK;

  naked_ns = loctable->get_naked_node(ns.id);
  assert(naked_ns);

  if (naked_ns->is_succ) {
    loctable->add_node(n,true); //accidentally discovered a new successor
    return;
  }else if (!naked_ns->fs) 
    goto NEXT_CHECK;

  //assert(naked_ns->status == LOC_HEALTHY);
  if (!naked_ns->fs) return;

  if (ConsistentHash::between(naked_ns->fs,naked_ns->fe,n.id)) {
    if ((t->latency(me.ip,ns.ip) > t->latency(me.ip,n.ip)) || (naked_ns->status==LOC_ONCHECK)) {
      loctable->add_node(n,false,true,naked_ns->fs,naked_ns->fe); //a better finger in latency
      loctable->del_node(ns,true);
      return;
    }else{
      learntable->add_node(n);
      return;
    }
  } 

NEXT_CHECK:
  IDMap ps = loctable->pred(n.id-1,LOC_ONCHECK);
  LocTable::idmapwrap *naked_ps = loctable->get_naked_node(ps.id);
  if (naked_ps && naked_ns && (!naked_ps->is_succ)) {
    if ((naked_ps->fe == naked_ns->fs) || ConsistentHash::between(naked_ps->fs,naked_ps->fe,n.id)) {
      //good good
      if (t->latency(me.ip,ns.ip) > t->latency(me.ip,n.ip)) {
	loctable->add_node(n,false,true,naked_ps->fs,naked_ps->fe);
	loctable->del_node(ns,true);
	return;
      }
    }else{
      learntable->add_node(n);
      return;
    }
  }
  ConsistentHash::CHID fs,fe;

  which_finger(_base,n.id,me.id,fs,fe);
  loctable->add_node(n,false,true,fs,fe);
}

bool
ChordFingerPNS::replace_node(IDMap n, IDMap &replacement)
{
  assert(learntable);
  LocTable::idmapwrap *naked = loctable->get_naked_node(n.id);
  if (!naked)
    return false;
  if (naked->is_succ) {
    //printf("%s no replacement for successor list %u,%qx\n",ts(), n.ip,n.id);
    //no replacement
    return false;
  }else{
    vector<IDMap> v = learntable->succs(naked->fs,_samples);
    IDMap min_f = me;
    Time min_lat = 10000000;
    Topology *t = Network::Instance()->gettopology();
    for (uint i = 0; i < v.size(); i++) {
      if (ConsistentHash::between(naked->fs,naked->fe,v[i].id)) { 
	if (min_lat > t->latency(me.ip,v[i].ip) && v[i].ip!=n.ip) {
	  min_lat = t->latency(me.ip,v[i].ip);
	  min_f = v[i];
	}
      }else{
	break;
      }
    }
    if (min_f.ip!=me.ip) {
      if (t->latency(me.ip,min_f.ip) < t->latency(me.ip,n.ip)) 
	loctable->add_node(min_f,false,true,naked->fs,naked->fe);
      else
	loctable->add_node(min_f,false,true,naked->fs,naked->fe,true);
      loctable->del_node(n);
      learntable->del_node(min_f,true);
      replacement = min_f;
      assert(min_f.ip != 0);
      //printf("%s replaced finger %u,%qx range (%qx,%qx) latency %u with %u,%qx latency %u\n", ts(), n.ip, n.id, naked->fs, naked->fe, (uint) t->latency(me.ip, n.ip), replacement.ip, replacement.id, (uint)t->latency(me.ip,replacement.ip));
      return true;
    }else{
      //printf("%s not replaced finger %u,%qx range (%qx,%qx) latency %u\n",ts(), n.ip,n.id, naked->fs,naked->fe, (uint)t->latency(me.ip, n.ip));
    }

  }
  return false;
}


void
ChordFingerPNS::initstate()
{
  vector<IDMap> ids = ChordObserver::Instance(NULL)->get_sorted_nodes();
  uint sz = ids.size();
  uint my_pos = find(ids.begin(), ids.end(), me) - ids.begin();
  assert(my_pos < sz && ids[my_pos].id == me.id);
  CHID min_lap = ids[(my_pos+_nsucc) % sz].id - me.id;

  CHID lap = (CHID) -1;

  Topology *t = Network::Instance()->gettopology();
  while (lap > min_lap) {
    lap = lap / _base;
    for (uint j = 1; j <= (_base -1); j++) {
      if (lap * j < min_lap) continue;
      IDMap tmpf;
      tmpf.id = lap * j + me.id;
      uint s_pos = upper_bound(ids.begin(), ids.end(), tmpf, Chord::IDMap::cmp) - ids.begin();

      s_pos = s_pos % sz;
      tmpf.id = lap * (j+1) + me.id + 1;
      uint e_pos = upper_bound(ids.begin(), ids.end(), tmpf, Chord::IDMap::cmp) - ids.begin();
      e_pos = e_pos % sz;
      if (s_pos == e_pos) continue;

      IDMap min_f;
      min_f.ip = 0;
      uint min_l = 1000000;
      hash_map<IPAddress, bool> inserted;

      for (uint x = 0; x < _fingerlets; x++) {
	int k = 0;
	for (uint i = s_pos; i!= e_pos; i = (i+1)%sz) {
	  if (inserted.find(ids[i].ip)==inserted.end() && t->latency(me.ip,ids[i].ip) < min_l) { 
	    min_f = ids[i];
	    min_l = t->latency(me.ip, ids[i].ip);
	  }
	  k++;
	  if (_samples > 0 && k>= _samples) break;
	}
	if (min_f.ip > 0) {
	  if (x == 0) 
	    loctable->add_node(min_f,false,true,tmpf.id,tmpf.id+lap);
	  else
	    learntable->add_node(min_f,false,true,tmpf.id,tmpf.id+lap);
	  inserted[min_f.ip] = true;
	  min_f.ip = 0;
	  min_l = 1000000;
	} else {
	  break;
	}
      }
    }
  }

#ifdef CHORD_DEBUG
  printf("chordfingerpns %u init_state %d _samples %d\n", me.ip, loctable->size(), _samples);
#endif
  Chord::initstate();
}

bool
ChordFingerPNS::stabilized(vector<CHID> lid)
{
  return true;
}

void
ChordFingerPNS::join(Args *args)
{

  if ((static_sim) || (!alive())) return;

  Chord::join(args);

  if (me.ip!=_wkn.ip) {
    CHID fs,fe;
    which_finger(_base, _wkn.id, me.id, fs, fe);
    learntable->add_node(_wkn,false,true,fs,fe);
  }

#ifdef CHORD_DEBUG
  if (me.ip == DNODE) 
    printf("%s joincrash pns stabilizer scheduled? %d\n",ts(),_stab_pns_running?1:0);
#endif

  //schedule pns stabilizer, no finger stabilizer
  if (!_stab_pns_running) {
    _stab_pns_running = true;
    if (!_learn)
      reschedule_pns_stabilizer((void *)1);
  }else if (_join_scheduled == 0){ //successfully joined
    if (!_learn)
      ChordFingerPNS::fix_pns_fingers(true);
  }
}

void
ChordFingerPNS::reschedule_pns_stabilizer(void *x)
{
  if (!alive()) {
    _stab_pns_running = false;
    return;
  }
  _stab_pns_running = true;
  if (_stab_pns_outstanding > 0) {
  }else{
    _stab_pns_outstanding++;
    fix_pns_fingers(x > 0?true:false);
    _stab_pns_outstanding--;
    assert(_stab_pns_outstanding == 0);
  }
  delaycb(_stab_pns_timer, &ChordFingerPNS::reschedule_pns_stabilizer, (void *) 0);
}

uint
ChordFingerPNS::num_live_samples(vector<IDMap> ss)
{
  uint num = 0;
  for (uint i = 0; i < ss.size(); i++) {
    if (getpeer(ss[i].ip)->alive()) {
      num++;
    }
  }
  return num;
}

void
ChordFingerPNS::fix_pns_fingers(bool restart)
{
  vector<IDMap> scs = loctable->succs(me.id + 1, _nsucc);
#ifdef CHORD_DEBUG
  printf("%s ChordFingerPNS stabilize BEFORE ring sz %u succ %d restart %u _inited? %d\n", ts(), loctable->size(), scs.size(), restart?1:0, _inited?1:0);
#endif
  uint dead_finger = 0;
  uint missing_finger = 0;
  uint real_missing_finger = 0;
  uint new_finger = 0;
  uint new_added_finger = 0;
  uint new_deleted_finger = 0;
  uint total_finger= 0;
  uint valid_finger = 0;
  uint skipped_finger = 0;
  uint finger_lookup = 0;
#ifdef CHORD_DEBUG
  vector<IDMap> testf;
  testf.clear();
#endif
  hash_map<IPAddress,bool> inserted;

  if (scs.size() == 0) return;

  vector<IDMap> v;
  vector<IDMap> newsucc;
  CHID finger;
  IDMap currf, prevf, prevfpred, deadf, min_f, tmp;
  bool ok;
  uint min_l;

  Topology *t = Network::Instance()->gettopology();

  CHID lap = (CHID) -1;

  prevf.ip = 0;
  prevfpred.ip = 0;

  IDMap pred = loctable->pred(me.id-1);
  IDMap haha = scs[scs.size()-1];

  while (1) {
    lap = lap/_base;
    for (uint j = (_base-1); j >= 1; j--) {
      finger = lap * j + me.id;
      if ((ConsistentHash::betweenrightincl(finger,finger+lap,scs[scs.size()-1].id)) || (!alive()))
	goto PNS_DONE;
      currf = loctable->succ(finger);
      if (currf.ip == me.ip) 
	currf.ip = 0;

      if (currf.ip) {
	LocTable::idmapwrap *naked = loctable->get_naked_node(currf.id);
	if (naked->status == LOC_REPLACEMENT) {
	  printf("%s re-install replaced crap %u,%qx\n",ts(),currf.ip,currf.id);
	  loctable->del_node(currf);
	  currf.ip = 0;
	}
      }
#ifdef CHORD_DEBUG
      testf.push_back(currf);//testing
#endif
      total_finger++;//testing
      //
      if (currf.ip == pred.ip) currf.ip = 0;
      //if (currf.ip == me.ip) continue;

      if ((!restart) && (currf.ip)) {

	if (ConsistentHash::between(finger, finger + lap, currf.id)) {
	  LocTable::idmapwrap *naked = loctable->get_naked_node(currf.id);
	  if (naked->is_succ) goto PNS_DONE;
	  assert(naked);
	  if (!naked->fs) {
	    naked->fs = finger;
	    naked->fe = finger+lap;
	  }
	  //if lookup has updated the timestamp of this finger, ignore it
	  if ((now() - naked->timestamp) < _stab_pns_timer) {
	    skipped_finger++; //testing
#ifdef CHORD_DEBUG
	    if (me.ip == DNODE) 
	      printf("%s DNODE skipping finger %u,%qx,%qx (%u)\n", ts(),j,lap,finger,currf.ip);
#endif
	    continue;
	  }
	  assert(currf.ip != prevf.ip);
	  prevf = currf;
	  prevfpred.ip = 0;
	  //just ping this finger to see if it is alive
	  //record_stat(TYPE_PNS_UP,0);
	  ok = failure_detect(currf, &Chord::null_handler, (void *)NULL,&currf,TYPE_PNS_UP,0,0);
	  if (!alive()) goto PNS_DONE;
#ifdef CHORD_DEBUG
	  if (me.ip == DNODE) 
	      printf("%s DNODE ping finger %u,%qx,%qx (%u) alive? %d \n", ts(),j,lap,finger,currf.ip, ok?1:0);
#endif

	  if(ok) {
	    record_stat(TYPE_PNS_UP,0);
	    loctable->update_ifexists(currf);//update timestamp
	    valid_finger++;//testing
	    continue;
	  }else{
	    loctable->del_node(currf);
	    naked = NULL;
	    dead_finger++;//testing
	    deadf = currf;
	    inserted[deadf.ip] = true;
	  }
	} else if (!restart) {
	  missing_finger++;//testing
	  if ((currf.ip == prevf.ip) && (prevfpred.ip) && (ConsistentHash::between(prevfpred.id,currf.id,finger))) {
#ifdef CHORD_DEBUG
	    if (me.ip == DNODE) 
	      printf("%s DNODE skip missing finger %u,%qx,%qx coz finger %u,%qx's pred is %u,%qx\n",ts(),j,lap,finger,currf.ip,currf.id,prevfpred.ip,prevfpred.id);
#endif
	    skipped_finger++;
	    continue;
	  } else if ((currf.ip != prevf.ip) || (!prevfpred.ip)) {
	    prevf = currf;
	    prevfpred.ip = 0;
	    //get predecessor, coz new finger within the candidate range might show up
	    get_predsucc_args gpa;
	    gpa.pred = true;
	    gpa.m=0;
	    get_predsucc_ret gpr;
	    record_stat(TYPE_PNS_UP,0);
	    ok = doRPC(currf.ip, &Chord::get_predsucc_handler, &gpa, &gpr, TIMEOUT(me.ip,currf.ip));
	    if (!alive()) goto PNS_DONE;
	    if(ok) {
	      record_stat(TYPE_PNS_UP,1);
	      loctable->add_node(currf);//update timestamp
	      prevfpred = gpr.n;
	      if (ConsistentHash::between(gpr.n.id,currf.id, finger)) { //pred is beyond finger, there is no real missing finger
#ifdef CHORD_DEBUG
		if (me.ip == DNODE) 
		  printf("%s DNODE skip missing finger %u,%qx,%qx currf %u,%qx's pred %u,%qx\n",ts(),j,lap,finger, currf.ip,currf.id,prevfpred.ip, prevfpred.id);
#endif
		continue;
	      } else {
#ifdef CHORD_DEBUG
		if (me.ip == DNODE) 
		  printf("%s DNODE real missing finger %u,%qx,%qx\n",ts(),j,lap,finger);
#endif
		real_missing_finger++; //testing
	      }
	    }else{
#ifdef CHORD_DEBUG
	      if (me.ip == DNODE) 
		printf("%s DNODE missing finger %u,%qx,%qx dead finger %u,%qx\n",ts(),j,lap,finger,currf.ip,currf.id);
#endif
	      loctable->del_node(currf);
	      deadf = currf;
	      inserted[deadf.ip] = true;
	      dead_finger++; //testing
	    }
	  }else{
#ifdef CHORD_DEBUG
	    if (me.ip == DNODE) 
	      printf("%s DNODE missing finger %u,%qx,%qx dead finger %u,%qx\n",ts(),j,lap,finger,currf.ip,currf.id);
#endif
	    real_missing_finger++; //testing
	  }
	}
      }
#ifdef CHORD_DEBUG      
      if (me.ip == DNODE) 
	printf("%s DNODE start to find finger %u,%qx,%qx\n",ts(),j,lap,finger);
#endif

      finger_lookup++;
      if (_recurs)
	v = find_successors_recurs(finger, _samples, TYPE_FINGER_LOOKUP);
      else
	v = find_successors(finger, _samples, TYPE_FINGER_LOOKUP);

      if (!alive()) goto PNS_DONE;
 
      if (v.size() > 0) {
	new_finger++; //testing

	uint x = 0;
	while (x < _fingerlets) {
	  min_l = 1000000;
	  min_f.ip = 0;
	  for (uint k = 0; k < v.size(); k++) {
	    if (ConsistentHash::between(finger,finger+lap,v[k].id)
	      && t->latency(me.ip, v[k].ip) < min_l && inserted.find(v[k].ip)==inserted.end()) {
	      min_f = v[k];
	      min_l = t->latency(me.ip, v[k].ip);
	    }
	  }

	  if (min_f.ip == 0) break;
	  newsucc = loctable->succs(me.id+1, _nsucc, LOC_HEALTHY);
	  if ((min_f.ip) && newsucc.size() && (!ConsistentHash::betweenrightincl(finger,finger+lap,newsucc[newsucc.size()-1].id))) {
	    new_added_finger++; //testing
	    assert(ConsistentHash::between(finger,finger+lap,min_f.id));
	    //ping this node, coz it might have been dead
	    record_stat(TYPE_PNS_UP,0); 
	    ok = doRPC(min_f.ip, &Chord::null_handler, (void *)NULL, &min_f, TIMEOUT(me.ip,min_f.ip));
	    if (!alive()) goto PNS_DONE;
	    if (ok) {
	      record_stat(TYPE_PNS_UP,0); 
	      if (x == 0) {
		loctable->add_node(min_f,false,true,finger,finger+lap);
#ifdef CHORD_DEBUG
		if (me.ip == DNODE) 
		  printf("%s DNODE ADD new finger %u,%qx,%qx (%u,%qx)\n",ts(),j,lap,finger,min_f.ip,min_f.id);
#endif
		tmp = loctable->succ(finger,LOC_HEALTHY);
		while (tmp.ip != min_f.ip) {
		  if (ConsistentHash::between(finger,finger+lap,tmp.id)) {
#ifdef CHORD_DEBUG
		    if (me.ip == DNODE) 
		      printf("%s DNODE DEL extra finger %u,%qx,%qx (%u,%qx)\n",ts(),j,lap,finger,tmp.ip,tmp.id);
#endif
		    loctable->del_node(tmp,true);
		    new_deleted_finger++; //testing
		  }else{
		    fprintf(stderr,"newsucc sz %u (%u,%qx) last new succ (%u,%qx)\n",newsucc.size(),newsucc[0].ip,newsucc[0].id,newsucc[newsucc.size()-1].ip,newsucc[newsucc.size()-1].id);
		    assert(0);
		  }
		  tmp = loctable->succ(finger,LOC_HEALTHY);
		}
	      }else{
		learntable->add_node(min_f,false,true,finger,finger+lap);
	      }
	      x++;
	    }
	    inserted[min_f.ip] = true;
	  }else
	    break;
	}
      } else{
#ifdef CHORD_DEBUG
	if (me.ip == DNODE) 
	  printf("%s DNODE failed to find finger %u,%qx,%qx\n",ts(),j,lap,finger);
#endif
      }

    }
  }
PNS_DONE:
#ifdef CHORD_DEBUG
  printf("%s ChordFingerPNS stabilize AFTER ring sz %u restart %d\n", ts(), loctable->size(), restart?1:0);
  if (me.ip == 1) {
    printf("loctable stat tested fingers: ");
    for (uint i = 0; i < testf.size(); i++) {
      printf("(%u,%qx) ", testf[i].ip, testf[i].id);
    }
    printf("\n");
  }

  if (me.ip == DNODE) {
    printf("%s DNODE after loctable stat finger_lookup %u total_finger %d valid_finger %d skipped_finger %d dead_finger %d missing_finger %d real_missing_finger %d new_finger %d new_added_finger %d new_deleted_finger %d\n", 
      ts(), finger_lookup, total_finger, valid_finger, skipped_finger, dead_finger, missing_finger, real_missing_finger, new_finger, new_added_finger, new_deleted_finger);
  }else{
    printf("%s after loctable stat finger_lookup %u total_finger %d valid_finger %d skipped_finger %d dead_finger %d missing_finger %d real_missing_finger %d new_finger %d new_added_finger %d new_deleted_finger %d\n", 
      ts(), finger_lookup, total_finger, valid_finger, skipped_finger, dead_finger, missing_finger, real_missing_finger, new_finger, new_added_finger, new_deleted_finger);
  }
#endif
  return;
}

void
ChordFingerPNS::dump()
{
  Chord::dump();
  IDMap succ = loctable->succ(me.id + 1,LOC_ONCHECK);
  CHID min_lap = succ.id - me.id;
  CHID lap = (CHID) -1;
  CHID finger;

  Topology *t = Network::Instance()->gettopology();

  while (lap > min_lap) {
    lap = lap / _base;
    for (uint j = 1; j <= (_base - 1); j++) {
      if ((lap * j) < min_lap) continue;
      finger = lap * j + me.id;
      succ = loctable->succ(finger,LOC_HEALTHY);
      if (succ.ip > 0) 
        printf("%qx: finger: %qx,%d : %qx : succ %qx lat %d\n", me.id, lap, j, finger, succ.id, (unsigned) t->latency(me.ip, succ.ip));
    }
  }
}
