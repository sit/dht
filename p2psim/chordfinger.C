#include  "chordfinger.h"

extern bool static_sim;

ChordFinger::ChordFinger(Node *n, Args &a,
			 LocTable *l) : Chord(n,a,l)  
{
  //get base
  if (a.find("base") != a.end()) {
    _base = atoi(a["base"].c_str());
  }else{
    _base = 2;
  }

  CHID finger;
  CHID lap = (CHID) -1;
  _numf = 0;
  while (lap > _base) {
    lap = lap/_base;
    for (unsigned int j = 1; j <= (_base- 1); j++) {
      finger = lap * j + me.id;
      loctable->pin(finger, 1, 0);
      _numf++;
    }
  }
}
void
ChordFinger::fix_fingers()
{
  IDMap succ = loctable->succ(me.id + 1);

  if (succ.ip == 0 || succ.id == me.id) return;
  unsigned int i0 = (uint) ConsistentHash::log_b(succ.id - me.id, 2);

  vector<Chord::IDMap> v;
  CHID finger;
  Chord::IDMap currf;
  bool ok;

  CHID lap = (CHID) -1;
  CHID min_lap = succ.id - me.id;
  while (lap > min_lap) {
    lap = lap/_base;
    for (uint j = 1; j <= (_base-1); j++) {
      finger = lap * j + me.id;
      currf = loctable->succ(finger);
      if (ConsistentHash::between(finger, finger+lap, currf.id )) {
	//just ping this finger to see if it is alive
	record_stat();
	if (_vivaldi) {
	  Chord *target = dynamic_cast<Chord*>(getpeer(currf.ip));
	  ok = _vivaldi->doRPC(currf.ip, target, &Chord::null_handler, 
	      (void*)NULL, (void *)NULL);
	}else
	  ok = doRPC(currf.ip, &Chord::null_handler, 
	      (void *)NULL, (void *)NULL);

	if (!ok) {
	  loctable->del_node(currf);
	} else {
	  loctable->add_node(currf);//update timestamp
	  continue;
	}
      }
    
      v = find_successors(finger, 1, false);
#ifdef CHORD_DEBUG
      printf("%s fix_fingers %d finger (%qx) get (%u,%qx)\n", ts(), i, finger, 
	v[0].ip, v[0].id);
#endif
      if (v.size() > 0) loctable->add_node(v[0]);
    }
  }
}

void
ChordFinger::reschedule_stabilizer(void *x)
{
  // printf("%s start stabilizing\n",ts());
  if (!node()->alive()) {
    _stab_running = false;
    return;
  }

  Time t = now();
  ChordFinger::stabilize();
  // printf("%s end stabilizing\n",ts());

  t = now() - t - _stabtimer;
  if (t < 0) t = 0;
  delaycb(_stabtimer, &ChordFinger::reschedule_stabilizer, (void *)0);
}

void
ChordFinger::stabilize()
{
  Chord::stabilize();
  fix_fingers();
}

bool
ChordFinger::stabilized(vector<CHID> lid)
{
  bool ret = Chord::stabilized(lid);
  if (!ret) return ret;

  uint sz = lid.size();
  uint my_pos = find(lid.begin(), lid.end(), me.id) - lid.begin();
  assert(lid[my_pos] == me.id);
  CHID min_lap = lid[(my_pos+1) % sz] - me.id;

  vector<CHID>::iterator it;
  CHID finger;
  uint pos;

  CHID lap = (CHID) -1;

  IDMap succ;
  uint numf = 0;
  while (lap > min_lap) {
    lap = lap / _base;
    for (uint j = 1; j <= (_base - 1); j++) {
      if ((lap * j) < min_lap) continue;
      finger = lap * j + me.id;
      it = upper_bound(lid.begin(), lid.end(), finger);
      pos = it - lid.begin();
      if (pos >= lid.size()) {
	pos = 0;
      }
      succ = loctable->succ(finger);
      if (lid[pos] != succ.id) {
	// printf("%s not stabilized, %qx,%d finger (%qx) should be %qx instead of (%u,%qx)\n", ts(), lap, j, finger, lid[pos], succ.ip, succ.id); 
	return false;
      }
      numf++;
    }
  }
  return true;
}

void
ChordFinger::dump()
{
  Chord::dump();
  IDMap succ = loctable->succ(me.id + 1);
  CHID min_lap = succ.id - me.id;
  CHID lap = (CHID) -1;
  CHID finger;

  while (lap > min_lap) {
    lap = lap / _base;
    for (uint j = 1; j <= (_base - 1); j++) {
      if ((lap * j) < min_lap) continue;
      finger = lap * j + me.id;
      succ = loctable->succ(finger);
      if (succ.ip > 0)  {
        // printf("%qx: finger: %qx,%d : %qx : succ %qx\n", me.id, lap, j, finger, succ.id);
      }
    }
  }
}
