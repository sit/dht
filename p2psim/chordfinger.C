#include  "chordfinger.h"
#include <stdio.h>
#include <iostream>
#include <algorithm>
using namespace std;

#define BASE 2

ChordFinger::ChordFinger(Node *n) : Chord(n) 
{
  uint level = (uint) ConsistentHash::log_b((CHID)-1, BASE);
  CHID finger;
  CHID lap = 1;
  uint num = 0;
  for (unsigned int i = 0; i < level; i++) {
    for (unsigned int j = 1; j <= (BASE - 1); j++) {
      finger = lap * j + me.id;
      loctable->pin(finger, 1, 0);
      num++;
    }
    lap = (lap * BASE);
  }
}

//this is a hack to make sure the number of entries in the loctable 
//is exactly some number of pre-specified maximum
void
ChordFinger::fix_fingers()
{
  IDMap succ = loctable->succ(me.id + 1);

  if (succ.ip == 0 || succ.id == me.id) return;
  unsigned int i0 = (uint) ConsistentHash::log_b(succ.id - me.id, 2);
  
  //
  vector<Chord::IDMap> v;
  CHID finger;
  for (unsigned int i = i0; i < NBCHID; i++) {
    finger = ConsistentHash::successorID(me.id,i);
    v = find_successors(finger, 1, true);
#ifdef CHORD_DEBUG
    printf("%s fix_fingers %d finger (%qx) get (%u,%qx)\n", ts(), i, finger, v[0].ip, v[0].id);
#endif
    if (v.size() > 0) loctable->add_node(v[0]);
  }
}

void
ChordFinger::reschedule_stabilizer(void *x)
{
  ChordFinger::stabilize();
  delaycb(STABLE_TIMER, &ChordFinger::reschedule_stabilizer, (void *)0);
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

  uint level = (uint) ConsistentHash::log_b((CHID)-1, BASE);
  CHID lap = 1;

  IDMap succ;
  for (uint i = 0; i < level; i++) {
    for (uint j = 1; j <= (BASE - 1); j++) {
      if (lap < min_lap) continue;
      finger = lap * j + me.id;
      it = upper_bound(lid.begin(), lid.end(), finger);
      pos = it - lid.begin();
      if (pos >= lid.size()) {
	pos = 0;
      }
      succ = loctable->succ(finger);
      if (lid[pos] != succ.id) {
	printf("%s not stablized, %d finger (%qx) should be %qx instead of (%u,%qx)\n", ts(), i, finger, lid[pos], succ.ip, succ.id); 
	return false;
      }
    }
    lap = (lap * BASE);
  }
  return true;
}

void
ChordFinger::dump()
{
  Chord::dump();
  IDMap succ = loctable->succ(me.id + 1);
  unsigned int i0 = (uint) ConsistentHash::log_b(succ.id - me.id,2);
  CHID finger;
  for (unsigned int i = i0; i < NBCHID; i++) {
    finger = ConsistentHash::successorID(me.id,i);
     succ = loctable->succ(finger);
     if (succ.ip > 0) 
       printf("%qx: finger: %d : %qx%s : succ %qx%s\n", me.id, i, finger, PAD, succ.id, PAD);
  }
}
