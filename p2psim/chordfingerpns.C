#include  "chordfingerpns.h"
#include <stdio.h>

/* Gummadi's Chord PNS algorithm  (static) */
ChordFingerPNS::ChordFingerPNS(Node *n, Args& a, LocTable *l) 
  : Chord(n, a, l) 
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
      IDMap min_f = me;
      IDMap min_f_pred = me;
      uint min_l = 100000000;
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
      printf("%s finger %d distance %d\n", ts(), ++f, min_l);
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
