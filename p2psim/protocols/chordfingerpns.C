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

/* Gummadi's Chord PNS algorithm  (static) */
ChordFingerPNS::ChordFingerPNS(IPAddress i, Args& a, LocTable *l) 
  : Chord(i, a, New LocTable()) 
{ 
  _base = a.nget<uint>("base",2,10);
  _samples = a.nget<int>("samples",_nsucc,10);

  _stab_pns_running = false;
  _stab_pns_outstanding = 0;

  _stab_pns_timer = a.nget<uint>("pnstimer",_stab_succlist_timer,10);

  if (!static_sim)
    loctable->set_timeout( 5*_stab_pns_timer );
}

void
ChordFingerPNS::oracle_node_died(IDMap n)
{
  Chord::oracle_node_died(n);

  IDMap tmp = loctable->succ(n.id);
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

  IDMap tmp = loctable->succ(n.id);
  if (tmp.ip == n.ip) return;

  //is the new node one of my finger?
  vector<IDMap> ids = ChordObserver::Instance(NULL)->get_sorted_nodes();
  uint sz = ids.size();
  CHID lap = (CHID) -1;
  while (lap > 1) {
    lap = lap / _base;
    for (uint j = 1; j <= (_base-1); j++) {
      if (ConsistentHash::between(me.id+lap*j, me.id+lap*(j+1), n.id)) {
	IDMap s = loctable->succ(me.id + lap * j);
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
	if (n_pos - s_pos <= _samples) {
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
      tmpf.id = lap * (j+1) + me.id;
      uint e_pos = upper_bound(ids.begin(), ids.end(), tmpf, Chord::IDMap::cmp) - ids.begin();
      e_pos = e_pos % sz;
      if (s_pos == e_pos) continue;

      IDMap min_f = ids[s_pos];
      IDMap min_f_pred = ids[(s_pos-1)%sz];
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
      loctable->add_node(min_f);//add pns finger
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

  Chord::join(args);
  if (static_sim) return;

  //schedule pns stabilizer, no finger stabilizer
  if (!_stab_pns_running) {
    _stab_pns_running = true;
    reschedule_pns_stabilizer((void *)1);
  }else{
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

void
ChordFingerPNS::fix_pns_fingers(bool restart)
{
  vector<IDMap> scs = loctable->succs(me.id + 1, _nsucc);
#ifdef CHORD_DEBUG
  printf("%s ChordFingerPNS stabilize BEFORE ring sz %u succ %d restart %u\n", ts(), loctable->size(), scs.size(), restart?1:0);
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
  vector<IDMap> testf;
  testf.clear();

  if (scs.size() == 0) return;

  vector<IDMap> v;
  vector<IDMap> newsucc;
  CHID finger;
  Chord::IDMap currf, prevf, prevfpred, deadf, min_f, tmp;
  Time currf_ts;
  bool ok;
  uint min_l;
  uint max_l;
  Topology *t = Network::Instance()->gettopology();

  CHID lap = (CHID) -1;

  prevf.ip = 0;
  prevfpred.ip = 0;

  while (1) {
    lap = lap/_base;
    for (uint j = (_base-1); j >= 1; j--) {
      finger = lap * j + me.id;
      if ((ConsistentHash::betweenrightincl(finger,finger+lap,scs[scs.size()-1].id)) || (!alive()))
	goto PNS_DONE;
      currf = loctable->succ(finger, &currf_ts);
      testf.push_back(currf);//testing
      total_finger++;//testing
      //
      if (currf.ip == me.ip) continue;

      if ((!restart) && (currf.ip)) { 

	if (ConsistentHash::between(finger, finger + lap, currf.id)) {
	  //if lookup has updated the timestamp of this finger, ignore it
	  if ((now() - currf_ts) < _stab_pns_timer) {
	    skipped_finger++; //testing
	    continue;
	  }
	  assert(currf.ip != prevf.ip);
	  prevf = currf;
	  prevfpred.ip = 0;
	  //just ping this finger to see if it is alive
	  record_stat(0, TYPE_PNS_UP);
	  ok = doRPC(currf.ip, &Chord::null_handler, (void *)NULL, (void *)NULL);
	  if(ok) {
	    record_stat(0, TYPE_PNS_UP);
	    loctable->add_node(currf);//update timestamp
	    valid_finger++;//testing
	    continue;
	  }else{
	    loctable->del_node(currf);
	    dead_finger++;//testing
	    deadf = currf;
	  }
	} else if (!restart) {
	  missing_finger++;//testing
	  if ((currf.ip == prevf.ip) && (prevfpred.ip) && (!ConsistentHash::between(finger, currf.id, prevfpred.id))) {
	    skipped_finger++;
	    continue;
	  } else if ((currf.ip != prevf.ip) || (!prevfpred.ip)) {
	    prevf = currf;
	    prevfpred.ip = 0;
	    //get predecessor, coz new finger within the candidate range might show up
	    get_predecessor_args gpa;
	    get_predecessor_ret gpr;
	    record_stat(0, TYPE_PNS_UP);
	    ok = doRPC(currf.ip, &Chord::get_predecessor_handler, &gpa, &gpr);
	    if(ok) {
	      record_stat(4, TYPE_PNS_UP);
	      loctable->add_node(currf);//update timestamp
	      prevfpred = gpr.n;
	      if (!ConsistentHash::between(finger, currf.id, gpr.n.id)) 
		continue;
	      else
		real_missing_finger++; //testing
	    }else{
	      loctable->del_node(currf);
	      deadf = currf;
	      dead_finger++; //testing
	    }
	  }else{
	    real_missing_finger++; //testing
	  }
	}
      }

      if (_recurs)
	v = find_successors_recurs(finger, _samples, _samples, TYPE_FINGER_LOOKUP);
      else
	v = find_successors(finger, _samples, _samples, TYPE_FINGER_LOOKUP);

      if (!alive()) return;

      if (v.size() > 0) {
	new_finger++; //testing
	max_l = 0;
	while (1) {
	  min_l = 1000000;
	  min_f = me;
	  for (uint k = 0; k < v.size(); k++) {
	    if (ConsistentHash::between(finger,finger+lap,v[k].id)
	      && t->latency(me.ip, v[k].ip) < min_l && deadf.ip != v[k].ip && t->latency(me.ip,v[k].ip) > max_l) {
	      min_f = v[k];
	      min_l = t->latency(me.ip, v[k].ip);
	    }
	  }
	  newsucc = loctable->succs(me.id+1, _nsucc);
	  if ((min_f.ip!=me.ip) && newsucc.size() && (!ConsistentHash::betweenrightincl(finger,finger+lap,newsucc[newsucc.size()-1].id))) {
	    new_added_finger++; //testing
	    assert(ConsistentHash::between(finger,finger+lap,min_f.id));
	    //ping this node, coz it might have been dead
	    record_stat(0,TYPE_FINGER_LOOKUP); 
	    ok = doRPC(min_f.ip, &Chord::null_handler, (void *)NULL, (void *)NULL);
	    if (ok) {
	      record_stat(0,TYPE_FINGER_LOOKUP); 
	      loctable->add_node(min_f);
	      tmp = loctable->succ(finger);
	      while (tmp.ip != min_f.ip) {
		if (ConsistentHash::between(finger,finger+lap,tmp.id)) {
		  loctable->del_node(tmp);
		  new_deleted_finger++; //testing
		}else{
		  assert(0);
		}
		tmp = loctable->succ(finger);
	      }
	      break;
	    }else{
	      max_l = min_l;
	    }
	  }else{
	    break;
	  }
	}
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

  printf("%s after loctable stat total_finger %d valid_finger %d skipped_finger %d dead_finger %d missing_finger %d real_missing_finger %d new_finger %d new_added_finger %d new_deleted_finger %d\n", 
      ts(), total_finger, valid_finger, skipped_finger, dead_finger, missing_finger, real_missing_finger, new_finger, new_added_finger, new_deleted_finger);
#endif
  return;
}

/*
void
ChordFingerPNS::my_next_recurs_handler(next_recurs_args *args, next_recurs_ret *ret)
{
  assert(args->type<=5);
  doRPC(me.ip, &ChordFingerPNS::pns_next_recurs_handler, args, ret);
}
*/
/*
void
ChordFingerPNS::pns_next_recurs_handler(next_recurs_args *args, next_recurs_ret *ret)
{
  vector<IDMap> succs;
  uint ssz, retsz,x,i;
  bool done;
  // bool r;
  lookup_path tmp;
  IDMap next;

  check_static_init();

  while (1) {
    succs = loctable->succs(me.id+1, _stab_succ);
    ssz = succs.size();

    if ((!_stab_succ) || (ssz < 1)) {
      ret->v.clear();
      return;
    }

    retsz = ret->v.size();
    for (x = 0; x < retsz; x++) {
      if (ret->v[x].ip  == me.ip) break;
    }
    if (x < retsz) {
      for (i = 0; i < ssz; i++) {
	if ((x + i + 1) < retsz) 
	  ret->v[x+i+1] = succs[i];
	else {
	  ret->v.push_back(succs[i]);
	  if (ret->v.size() == args->all) break;
	}
      }
      return;
    }
// for (uint i = 0; i < ssz; i++) {
//   if (!ConsistentHash::betweenrightincl(me.id, succs[i].id, args->key)) {
//   }else{
//     if ((ret->v.size() == 0) || ConsistentHash::between(args->key, succs[i].id, ret->v.back().id)) {
//	ret->v.push_back(succs[i]);
//	if (ret->v.size() == _allfrag) break;
//     }
//    }
//  }

  //if ((ret->v.size() >= args->m) || (ConsistentHash::between(me.id, succs[0].id,args->key)))

    if (ConsistentHash::between(me.id, succs[0].id,args->key)) {
      for (i = 0; i < succs.size(); i++) {
	ret->v.push_back(succs[i]);
	if (ret->v.size() >= args->m) return;
      }
      return;
    }else {
      if (args->path.size() > 20) {
	printf("%s WRONG!!!!!! key %qx: ",ts(),args->key);
	for (uint i = 0; i < args->path.size(); i++) 
	  printf("(%u,%qx,%u) ", args->path[i].n.ip, args->path[i].n.id, args->path[i].tout);
	printf("\n");
	assert(0);
      }
      // for args->m > 1:
      //XXX i never check if succ is dead or not
      //XXX this will break during dynamism. 
      // since next_hop assumes succ is on ret->v., when it breaks,
      // it does not attempt to update ret->v

      next = loctable->next_hop(args->key, &done, args->m, 1);
      assert(next.ip != me.ip);
      assert(!done);
      assert(ConsistentHash::between(me.id, args->key, next.id));

      tmp.n = next;
      tmp.tout = 0;
      args->path.push_back(tmp);

      record_stat(4+1,args->type?1:0);
      bool r = doRPC(next.ip, &ChordFingerPNS::pns_next_recurs_handler, args, ret);

      if (!alive()) {
	ret->v.clear();
	return;
      }

      if (r) {
	record_stat(4*ret->v.size(),args->type);
	loctable->add_node(next);
	return;
      } else {
#ifdef CHORD_DEBUG
	printf ("%s next hop to %u,%16qx failed\n", ts(), next.ip, next.id);
#endif
	args->path[args->path.size()-1].tout = 1;
	loctable->del_node(next);
	//restart this query
      }
    }
  }
}
*/

void
ChordFingerPNS::dump()
{
  Chord::dump();
  IDMap succ = loctable->succ(me.id + 1);
  CHID min_lap = succ.id - me.id;
  CHID lap = (CHID) -1;
  CHID finger;

  Topology *t = Network::Instance()->gettopology();

  while (lap > min_lap) {
    lap = lap / _base;
    for (uint j = 1; j <= (_base - 1); j++) {
      if ((lap * j) < min_lap) continue;
      finger = lap * j + me.id;
      succ = loctable->succ(finger);
      if (succ.ip > 0) 
        printf("%qx: finger: %qx,%d : %qx : succ %qx lat %d\n", me.id, lap, j, finger, succ.id, (unsigned) t->latency(me.ip, succ.ip));
    }
  }
}
