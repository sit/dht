#include  "chordfingerpns.h"
#include <stdio.h>

/* Gummadi's Chord PNS algorithm  (static) */
ChordFingerPNS::ChordFingerPNS(Node *n, Args& a, LocTable *l) 
  : Chord(n, a, new LocTablePNS()) 
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
  uint f = 0;
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
      int candidates = (e_pos - s_pos) % sz;
      double prob = (double)_samples/(double)(candidates);
      IDMap min_f = ids[s_pos];
      IDMap min_f_pred = ids[(s_pos-1)%sz];
      uint min_l = t->latency(me.ip,min_f.ip);
      if (_samples > 0 && candidates > 10 * _samples) {
	//use a more efficient sampling technique
	for (int j = 0; j < _samples; j++) {
	  uint i = uint ((((double)random()/(double)RAND_MAX) * candidates));
	  assert(i>= 0 && i < (uint) candidates);
	  if (t->latency(me.ip, ids[(s_pos + i) % sz].ip) < min_l) {
	    min_f = ids[(s_pos + i) % sz];
	    min_l = t->latency(me.ip, ids[(s_pos + i)%sz].ip);
	    min_f_pred = ids[(s_pos + i - 1) % sz];
	  }
	}
      } else {
	for (uint i = s_pos; i!= e_pos; i = (i+1)%sz) {
	  double r = (double) random()/(double)RAND_MAX;
	  if (_samples < 0 || r < prob) { //sample only _samples out of all candidates approximately
	    if (t->latency(me.ip,ids[i].ip) < min_l) {
	      min_f = ids[i];
	      min_l = t->latency(me.ip, ids[i].ip);
	      min_f_pred = ids[(i - 1) % sz];
	    }
	  }
	}
      }
      min_f.choices = ((candidates > 0)? candidates:1);
      printf("%s finger shit %d %u,%qx distance %d candidates %d\n", ts(), ++f, min_f.ip, min_f.id, min_l, min_f.choices);
      loctable->add_node(min_f);
//      ((LocTablePNS *)loctable)->add_finger(make_pair(min_f_pred, min_f));
      //Gummadi assumes the node knows the idspace each of the neighbors is 
      //responsible for. this is equivalent to knowing each of the neighbors' predecessor.
      //so add the predecessor for this finger
      //loctable->add_node(min_f_pred);
    }
  }

  _inited = true;
  //add successors and (each of the successor's predecessor)
  for (uint i = 1; i <= _nsucc; i++) {
    loctable->add_node(ids[(my_pos + i) % sz]);
  }
 // ((LocTablePNS *)loctable)->add_finger(make_pair(ids[my_pos %sz],ids[(my_pos+1)%sz]));
  //add predecessor
  loctable->add_node(ids[(my_pos-1) % sz]);
  printf("ChordFingerPNS::init_state (%u,%qx) loctable size %d\n", me.ip, me.id, loctable->size());

}

bool
ChordFingerPNS::stabilized(vector<CHID> lid)
{
  return true;
}

//damn!! play the optimal trick on lowest latency lookup+fetch
vector<Chord::IDMap>
ChordFingerPNS::find_successors_recurs(CHID key, uint m, bool is_lookup, uint *recurs_int)
{
  next_recurs_args fa;
  pns_next_recurs_ret fr;
  fr.v.clear();
  fa.path.clear();
  fa.path.push_back(me);
  fa.key = key;
  fa.m = m;
  fa.is_lookup = is_lookup;

  doRPC(me.ip, &ChordFingerPNS::pns_next_recurs_handler, &fa, &fr);

  Topology *t = Network::Instance()->gettopology();

  printf("%s pns_find_successors_recurs %qx route ",ts(),key);
  uint total_lat = 0;
  for (uint i = 0; i < fr.path.size(); i++) {
    IDMap n = fr.path[i];
    IDMap np = (i == 0)? fr.path[fr.path.size()-1]:fr.path[i-1];
    printf("(%u,%qx,%u,%u) ", n.ip, n.id, t->latency(np.ip, fr.path[i].ip), fr.path[i].choices);
    if ((i+2) <= fr.path.size()) {
      total_lat += 2 * t->latency(fr.path[i].ip,fr.path[i+1].ip);
    }
  }
  printf("\n");

  //calculate the optimal route
  vector<latency_t> lat;
  lat.clear();
  uint j = 0;
  latency_t total_forward = 0;

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
    lat.push_back(t->latency(me.ip, fr.v[i].first.ip));
  }
  sort(lat.begin(), lat.end());

  //now when is the lookup and when is the fetch is fuzzy
  printf("%s pns_recurs INTERVAL %u %u %u\n", ts(), lat[m-1], total_lat - lat[m-1], total_lat - lat[m-1]);

  return results;
}

void
ChordFingerPNS::pns_next_recurs_handler(next_recurs_args *args, pns_next_recurs_ret *ret)
{
  check_static_init();

  vector<IDMap> succs = loctable->succs(me.id+1, _nsucc);

  pair<IDMap,IDMap> succ;
  for (uint i = 0; i < succs.size(); i++) {
    if (!ConsistentHash::betweenrightincl(me.id, succs[i].id, args->key)) {
    }else{
      if ((ret->v.size() == 0) || ConsistentHash::between(args->key, succs[i].id, ret->v.back().first.id)) {
	succ.first = succs[i];
	succ.second = me;
	ret->v.push_back(succ);
      }
    }
  }

  if ((ret->v.size() >= _nsucc) 
      || (succs.size() < _nsucc)) { //this means there's < m nodes in the system 
    ret->path = args->path;
  }else {
#ifdef CHORD_DEBUG
    uint x = find(args->path.begin(), args->path.end(), me) - args->path.begin();
    if (x != (args->path.size() - 1)) {
      printf("%s error! back to me!\n", ts());
      for (uint xx = 0; xx < args->path.size(); xx++) {
	printf("route %u,%qx\n", args->path[xx].ip, args->path[xx].id);
      }
      abort();
    }
    printf("%s not enough succs to answer this query %d\n", ts(), ret->v.size());
#endif
    assert(args->path.size() < 100);

    //XXX i never check if succ is dead or not
    bool r = false;
    while (!r) {
      bool done;
      IDMap next = loctable->next_hop(args->key, &done, args->m, _nsucc);
      assert(!done);
      assert(next.choices > 0);

      args->path.push_back(next);

      r = doRPC(next.ip, &ChordFingerPNS::pns_next_recurs_handler, args, ret);

      assert(r);
      if ((!r) && (node()->alive())) {
	printf ("%16qx rpc to %16qx failed %llu\n", me.id, next.id, now ());
	args->path.pop_back();
	loctable->del_node(next);
      }
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
        printf("%qx: finger: %qx,%d : %qx : succ %qx lat %d\n", me.id, lap, j, finger, succ.id, t->latency(me.ip, succ.ip));
    }
  }
}
