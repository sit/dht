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

#include  "chordfinger.h"

extern bool static_sim;

ChordFinger::ChordFinger(Node *n, Args &a,
			 LocTable *l) : Chord(n,a,l)  
{
  _base = a.nget<uint>("base",2,10);

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

  _stab_finger_running = false;
  _stab_finger_outstanding = 0;
}

void
ChordFinger::init_state(vector<IDMap> ids)
{
  uint sz = ids.size();
  uint my_pos = find(ids.begin(), ids.end(), me) - ids.begin();
  assert(ids[my_pos].id == me.id);
  CHID min_lap = ids[(my_pos+1) % sz].id - me.id;
  CHID lap = (CHID) -1;
  IDMap tmpf;
  while (lap > min_lap) {
    lap = lap / _base;
    for (uint j = 1; j <= (_base -1); j++) {
      if (lap * j < min_lap) continue;
      tmpf.id = lap * j + me.id;
      uint s_pos = upper_bound(ids.begin(), ids.end(), tmpf, Chord::IDMap::cmp) - ids.begin();
      loctable->add_node(ids[s_pos]);
    }
  }
  Chord::init_state(ids);
}

void
ChordFinger::fix_fingers(bool restart)
{

  vector<IDMap> scs = loctable->succs(me.id + 1, _stab_succ>0?_stab_succ:1);

  printf("%s ChordFinger stabilize BEFORE _stab_succ %u ring sz %u succ %d\n", ts(), _stab_succ, loctable->size(), scs.size());

  if (scs.size() == 0) return;

  vector<IDMap> v;
  CHID finger;
  Chord::IDMap currf, prevf;
  bool ok;

  CHID lap = (CHID) -1;
  CHID min_lap = scs[scs.size()-1].id - me.id;

  prevf.ip = 0;

  while (1) {
    lap = lap/_base;
    for (uint j = (_base-1); j >= 1; j--) {
      if (lap * j < min_lap) goto FINGER_DONE;
      finger = lap * j + me.id;
      currf = loctable->succ(finger);
      if (currf.ip == me.ip) continue;

      if ((!restart) && (currf.ip == prevf.ip)) {
	//this finger is the same as the last one, skip it
	continue;
      }else if ((!restart) && (currf.ip)) { 
	prevf = currf;
	//just ping this finger to see if it is alive
	get_predecessor_args gpa;
	get_predecessor_ret gpr;
	record_stat(0, TYPE_FINGER_UP);
	if (_vivaldi) {
	  Chord *target = dynamic_cast<Chord*>(getpeer(currf.ip));
	  ok = _vivaldi->doRPC(currf.ip, target, &Chord::get_predecessor_handler, 
	      &gpa, &gpr);
	}else 
	  ok = doRPC(currf.ip, &Chord::get_predecessor_handler,
	      &gpa, &gpr);
	if(ok) record_stat(4, TYPE_FINGER_UP);

	if (!ok) {
	  loctable->del_node(currf);
	} else {
	  if (ConsistentHash::between(finger,finger+lap,gpr.n.id)) 
	    //the predecessor lies in the range, sth. fishy is going on, re-lookup finger
	    loctable->add_node(gpr.n);
	  else {
	    loctable->add_node(currf);//update timestamp
	    continue;
	  }
	}
      }
      if (_recurs)
	v = find_successors_recurs(finger, 1, 1, TYPE_FINGER_LOOKUP, NULL);
      else
	v = find_successors(finger, 1, 1, TYPE_FINGER_LOOKUP, NULL);

#ifdef CHORD_DEBUG
      if (v.size() > 0)
	printf("%s fix_fingers %d finger (%qx) get (%u,%qx)\n", ts(), j, finger, 
	    v[0].ip, v[0].id);
#endif
      if (v.size() > 0) loctable->add_node(v[0]);
    }
  }
FINGER_DONE:
  printf("%s ChordFinger stabilize AFTER _stab_succ %u ring sz %u\n", ts(), _stab_succ, loctable->size());
}

void
ChordFinger::join(Args *args)
{
  Chord::join(args);

  //schedule finger stabilizer
  if (!_stab_finger_running) {
    _stab_finger_running = true;
    reschedule_finger_stabilizer((void *)1); //a hack, no null means restart fixing fingres
  }else{
    ChordFinger::fix_fingers();
  }
}

void
ChordFinger::reschedule_finger_stabilizer(void *x)
{
  //printf("%s start stabilizing\n",ts());
  if (!node()->alive()) {
    _stab_finger_running = false;
    return;
  }

  _stab_finger_running = true;
  if (_stab_finger_outstanding > 0) {
  }else{
    _stab_finger_outstanding++;
    fix_fingers(x!=NULL);
    _stab_finger_outstanding--;
    assert(_stab_finger_outstanding == 0);
  }
  delaycb(_stabtimer, &ChordFinger::reschedule_finger_stabilizer, (void *)0);
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
