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

#include  "chordfingerpns.h"
#include <stdio.h>

/* Gummadi's Chord PNS algorithm  (static) */
ChordFingerPNS::ChordFingerPNS(Node *n, Args& a, LocTable *l) 
  : ChordFinger(n, a, New LocTablePNS()) 
{ 
  _base = a.nget<uint>("base",2,10);
  _samples = a.nget<uint>("samples",16,10);
}

void
ChordFingerPNS::init_state(vector<IDMap> ids)
{
  loctable->set_evict(false);

  uint sz = ids.size();
  uint my_pos = find(ids.begin(), ids.end(), me) - ids.begin();
  assert(ids[my_pos].id == me.id);
  CHID min_lap = ids[(my_pos+1) % sz].id - me.id;

  CHID lap = (CHID) -1;

  assert(_base == 2);

  Topology *t = Network::Instance()->gettopology();
  IDMap tmpf;
  while (lap > min_lap) {
    lap = lap / _base;
    for (uint j = 1; j <= (_base -1); j++) {
      if (lap * j < min_lap) continue;
      tmpf.id = lap * j + me.id;
      uint s_pos = upper_bound(ids.begin(), ids.end(), tmpf, Chord::IDMap::cmp) - ids.begin();

      s_pos = s_pos % sz;
      tmpf.id = lap * (j+1) + me.id;
      uint e_pos = upper_bound(ids.begin(), ids.end(), tmpf, Chord::IDMap::cmp) - ids.begin();
      e_pos = e_pos % sz;

      IDMap min_f = ids[s_pos];
      IDMap min_f_pred = ids[(s_pos-1)%sz];
      uint min_l = t->latency(me.ip,min_f.ip);
      uint j = 0;
      for (uint i = s_pos; i!= e_pos; i = (i+1)%sz) {
	if (t->latency(me.ip,ids[i].ip) < min_l) {
	  min_f = ids[i];
	  min_l = t->latency(me.ip, ids[i].ip);
	  j++;
	  if (j>= _samples) break;
	}
      }
      printf("%s finger %u,%qx distance %d candidates %d\n", ts(), min_f.ip, min_f.id, min_l, (e_pos-s_pos)%sz);
      loctable->add_node(ids[s_pos]);//add immediate finger
      loctable->add_node(min_f);//add pns finger
    }
  }
  _inited = true;

  //add successors and (each of the successor's predecessor)
  for (uint i = 1; i <= _nsucc; i++) {
    loctable->add_node(ids[(my_pos + i) % sz]);
  }

  //add predecessor
  loctable->add_node(ids[(my_pos-1) % sz]);

  printf("%s init_state loctable size %d\n", ts(), loctable->size());
  _stab_succ = _nsucc;
  ((LocTablePNS *)loctable)->rebuild_pns_finger_table(_base, _stab_succ);
}

bool
ChordFingerPNS::stabilized(vector<CHID> lid)
{
  return true;
}

void
ChordFingerPNS::reschedule_stabilizer(void *x)
{
  if (!node()->alive()) {
    _stab_running = false;
    return;
  }
  _stab_running = true;
  if (_stab_outstanding > 0) {
  }else{
    _stab_outstanding++;
    stabilize();
    _stab_outstanding--;
    assert(_stab_outstanding == 0);
  }
  delaycb(_stabtimer, &ChordFingerPNS::reschedule_stabilizer, (void *) 0);
}

void
ChordFingerPNS::stabilize()
{
  ChordFinger::stabilize();
  fix_pns_fingers();
}

void
ChordFingerPNS::fix_pns_fingers()
{
  get_successor_list_args gsa;
  get_successor_list_ret gsr;
  gsa.m = _samples;
  assert(_samples <= (int)_nsucc);

  vector<IDMap> scs = loctable->succs(me.id + 1,_stab_succ);
  if (scs.size() == 0) return;

  vector<Chord::IDMap> v;
  CHID finger;
  Chord::IDMap currf;
  bool ok;

  CHID lap = (CHID) -1;
  CHID min_lap = scs[scs.size()-1].id - me.id;
  while (lap > min_lap) {
    lap = lap/_base;
    for (uint j = 1; j <= (_base-1); j++) {
      finger = lap * j + me.id;
      currf = loctable->succ(finger);
      if ((currf.ip == 0) || (currf.ip == me.ip)) continue;
      //get the list of successors from it
      record_stat(0);
      if (_vivaldi) {
	Chord *target = dynamic_cast<Chord*>(getpeer(currf.ip));
	ok = _vivaldi->doRPC(currf.ip, target, &Chord::get_successor_list_handler, 
	    &gsa, &gsr);
      }else 
	ok = doRPC(currf.ip, &Chord::get_successor_list_handler,
	    &gsa, &gsr);
      if (ok) record_stat(4*gsr.v.size());

      if (!ok) 
	loctable->del_node(currf);
      else 
	loctable->add_node(currf);//update timestamp

      //add everything to PNS table
      for (uint i = 0; i < gsr.v.size(); i++) 
	loctable->add_node(gsr.v[i]);
    }
  }
  ((LocTablePNS *)loctable)->rebuild_pns_finger_table(_base, _stab_succ);
}

//the difference between this and vanilla chord is 
//it might contact one of the successor of the key 
//for successor lists to get more successors for the key
vector<Chord::IDMap>
ChordFingerPNS::find_successors(CHID key, uint m, bool is_lookup)
{
  assert(0); //this does not really work now, stop early?not stop early?
  assert(m <= _nsucc);

  int count = 0;

  vector<IDMap> route;

  next_args na;
  next_ret nr;

  na.key = key;
  na.m = _asap; 

  IDMap nprime = me;

  route.clear();
  route.push_back(me);

  uint timeout = 0;

  while(1){
    assert(count++ < 500);

    bool r;

    record_stat(4,is_lookup?1:0);
    if (_vivaldi) {
      Chord *target = dynamic_cast<Chord *>(getpeer(nprime.ip));
      r = _vivaldi->doRPC(nprime.ip, target, &Chord::next_handler, &na, &nr);
    } else
      r = doRPC(nprime.ip, &Chord::next_handler, &na, &nr);
    if (r) record_stat(nr.v.size()*4);

    if(nr.v.size() > 0){
      
      if ((int) nr.v.size() < _asap) {
	//get successor list from the best next candidate
	uint i;
	for (i = 0; i < nr.v.size(); i++) {
	  if (nr.v[i].ip == nr.next.ip) {
	    break;
	  }
	}
	if (i == nr.v.size()) {
	  //keep going
	}else{
	  get_successor_list_args gsa;
	  get_successor_list_ret gsr;
	  gsa.m = _nsucc;
	  record_stat(0);
	  bool ok = doRPC(nr.next.ip, &Chord::get_successor_list_handler, &gsa, &gsr);
	  if (ok) record_stat(gsr.v.size() * 4);
	  assert(ok);
	  IDMap last = nr.v[nr.v.size()-1];
	  assert(gsr.v.size() == _nsucc);
	  for (uint i = 0; i < gsr.v.size(); i++) {
	    if (ConsistentHash::betweenleftincl(key, gsr.v[i].id, last.id)) {
	      nr.v.push_back(gsr.v[i]);
	      if (nr.v.size() == _allfrag) break;
	    }
	  }
	if (nr.v.size() < m) {
	  fprintf(stderr,"%s key %16qx return list: ",ts(), key);
	  for (uint i = 0; i < nr.v.size(); i++) {
	    fprintf(stderr," (%d,%16qx)", nr.v[i].ip,nr.v[i].id);
	  }
	  fprintf(stderr,"\n");

	  fprintf(stderr,"%s last hop %d,%16qx (last %d): ",ts(), nr.next.ip, nr.next.id,last.ip);
	  for (uint i = 0; i < gsr.v.size(); i++) {
	    fprintf(stderr," (%d,%16qx)", gsr.v[i].ip,gsr.v[i].id);
	  }
	  fprintf(stderr,"\n");

	  abort();
	}
	  goto DONE;
	}
      }else{
	goto DONE;
      }
    }
    
    if (r) {

      if (route.size() > 0) 
	assert(nr.next.ip != route[route.size()-1].ip);

      route.push_back(nr.next);
      if (route.size() == 20) {
	fprintf(stderr,"%s key %16qx \n route: ", ts(), key);
	for (uint i = 0; i < route.size(); i++) {
	  fprintf(stderr," (%d,%16qx)",route[i].ip,route[i].id);
	}
	fprintf(stderr,"\n");
      }

      assert(route.size() < 20);
      nprime = nr.next;
      
    } else {
      assert(0); //Chordpns does not handle failure yet
    }
  }

DONE:
  if (is_lookup) {
    printf ("find_successor for (id %qx, key %qx)", me.id, key);
    if (nr.v.size () == 0) {
      printf ("failed\n");
    } else {
      printf ("is (%u, %qx) hops %d timeout %d\n", nr.v[0].ip, 
	  nr.v[0].id, route.size(), timeout);
    }
  }

#ifdef CHORD_DEBUG
  Topology *t = Network::Instance()->gettopology();
  if (nr.v.size() > 0) {
    printf("%s find_successors %qx route: ", ts(), key);
    for (uint i = 0; i < route.size(); i++) {
      printf("(%u,%qx,%u,%u) ", route[i].ip, route[i].id, 2*t->latency(me.ip,route[i].ip), route[i].choices);
    }
    printf("\n");
  }
#endif
  assert((int) nr.v.size() >= _asap);
  return nr.v;
}


/*
vector<Chord::IDMap>
ChordFingerPNS::find_successors_recurs(CHID key, uint m, bool is_lookup, uint *recurs_int)
{
  if (_asap < 0) return Chord::find_successors_recurs(key,m,is_lookup, recurs_int);

  pns_next_recurs_args fa;
  pns_next_recurs_ret fr;
  fr.v.clear();
  fa.path.clear();
  fa.path.push_back(me);
  fa.key = key;
  fa.is_lookup = is_lookup;
  fa.m = _asap;

  doRPC(me.ip, &ChordFingerPNS::pns_next_recurs_handler, &fa, &fr);

  Topology *t = Network::Instance()->gettopology();

  printf("%s pns_find_successors_recurs %qx route ",ts(),key);

  uint total_lat = 0;
  for (uint i = 0; i < fr.path.size(); i++) {
    IDMap n = fr.path[i];
    printf("(%u,%qx,%u,%u) ", n.ip, n.id, (unsigned) t->latency(n.ip,fr.path[(i+1)%fr.path.size()].ip), fr.path[(i+1)%fr.path.size()].choices);
    total_lat += t->latency(fr.path[i].ip, fr.path[(i+1)%fr.path.size()].ip);
  }
  printf("\n");

  if (recurs_int) {
    *recurs_int = total_lat;
  }
  
  uint frsz = fr.path.size();
  if (frsz > 1) {
    printf("%s %u last_hop %u\n", ts(), 
	(frsz>1)? (unsigned) t->latency(fr.path[frsz-2].ip,fr.path[frsz-1].ip):0);
  }
  printf("\n");

  //calculate the optimal route
  vector<Time> lat;
  lat.clear();
  uint j = 0;
  Time total_forward = 0;

  vector<IDMap> results;
  results.clear();
  for (uint i = 0; i < fr.v.size(); i++) {
    results.push_back(fr.v[i].first);
    while (true) {
      if (fr.path[j].ip == fr.v[i].second.ip) {
	break;
      }
      j++;
      assert(j < fr.path.size());
      total_forward += t->latency(fr.path[j-1].ip, fr.path[j].ip);
    }

    lat.push_back(total_forward + t->latency(me.ip,fr.path[j].ip) + 2 * t->latency(me.ip, fr.v[i].first.ip));
  }
  sort(lat.begin(), lat.end());
  total_lat = lat[m-1];

  lat.clear();
  for (uint i = 0; i < fr.v.size(); i++) {
    lat.push_back(2 * t->latency(me.ip, fr.v[i].first.ip));
  }
  sort(lat.begin(), lat.end());

  //now when is the lookup and when is the fetch is fuzzy
  if (is_lookup) {
    printf("%s lookup key %qx,%d, hops %d\n", ts(), key, m, fr.path.size());
    printf("%s pns_recurs INTERVAL %u %u %u\n", ts(), (unsigned) lat[m-1], (unsigned) total_lat - (unsigned) lat[m-1], (unsigned) total_lat - (unsigned) lat[m-1]);
  }

  return results;
}
*/
void
ChordFingerPNS::my_next_recurs_handler(next_recurs_args *args, next_recurs_ret *ret)
{
  doRPC(me.ip, &ChordFingerPNS::pns_next_recurs_handler, args, ret);
}

void
ChordFingerPNS::pns_next_recurs_handler(next_recurs_args *args, next_recurs_ret *ret)
{
  check_static_init();
  assert(args->m <= _allfrag);

  vector<IDMap> succs = loctable->succs(me.id+1, _stab_succ);

  if ((!_stab_succ) || (succs.size() < 1)) {
    ret->v.clear();
    return;
  }

  IDMap succ = succs[0];

  uint retsz = ret->v.size();
  for (uint i = 0; i < retsz; i++) {
    if (ret->v[i].ip  == me.ip) {
      //i just append my successor list to 
      if (retsz >= (i+2)) {
	assert(ret->v[retsz-1].ip == succs[retsz-2-i].ip);
      }
      for (uint j = retsz - i - 1; j < succs.size(); j++) {
	ret->v.push_back(succs[j]);
	if (ret->v.size() == _allfrag) break;
      } 
      return; 
    }
  }

  for (uint i = 0; i < succs.size(); i++) {
    if (!ConsistentHash::betweenrightincl(me.id, succs[i].id, args->key)) {
    }else{
      if ((ret->v.size() == 0) || ConsistentHash::between(args->key, succs[i].id, ret->v.back().id)) {
	ret->v.push_back(succs[i]);
	if (ret->v.size() == _allfrag) break;
      }
    }
  }

  if (ret->v.size() >= args->m) {
  }else {
    if (args->path.size() > 20) {
      printf("%s WRONG!!!!!! : ",ts());
      for (uint i = 0; i < args->path.size(); i++) 
	printf("(%u,%qx,%u) ", args->path[i].n.ip, args->path[i].n.id, args->path[i].tout);
      printf("\n");
      assert(0);
    }
    assert(args->path.size() < 100);

    // for args->m > 1:
    //XXX i never check if succ is dead or not
    //XXX this will break during dynamism. 
    // since next_hop assumes succ is on ret->v., when it breaks,
    // it does not attempt to update ret->v
    bool done;
    lookup_path tmp;

    IDMap next = loctable->next_hop(args->key, &done, args->m, _stab_succ);
    assert(next.ip != me.ip);
    assert(!done);

    tmp.n = next;
    tmp.tout = 0;
    args->path.push_back(tmp);

    record_stat(4,args->is_lookup?1:0);
    bool r = doRPC(next.ip, &ChordFingerPNS::pns_next_recurs_handler, args, ret);
    if (r) record_stat(4,ret->v.size());

    if (!node()->alive()) {
      ret->v.clear();
      return;
    }

    if (!r) {
      printf ("%s next hop to %u,%16qx failed\n", ts(), next.ip, next.id);
      args->path[args->path.size()-1].tout = 1;
      loctable->del_node(next);
      //restart this query
      pns_next_recurs_handler(args,ret);
    }
  }
}

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
