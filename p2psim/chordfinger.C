#include  "chordfinger.h"
#include <stdio.h>
#include <iostream>
#include <algorithm>
using namespace std;

static unsigned int 
log_2(ConsistentHash::CHID gap)
{
  unsigned int i = 0;
  while (gap > 0) {
    i++;
    gap = gap >>1;
  }
  return i;
}

ChordFinger::ChordFinger(Node *n) : Chord(n) 
{
  for (unsigned int i = 0; i < NBCHID; i++) {
    loctable->pin(ConsistentHash::successorID(me.id, i), 1, 0);
  }
}

void
ChordFinger::fix_fingers()
{
  IDMap succ = loctable->succ(me.id + 1);

  if (succ.ip == 0 || succ.id == me.id) return;
  unsigned int i0 = log_2(succ.id - me.id);
  
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

  IDMap succ = loctable->succ(me.id + 1);
  unsigned int i0 = log_2(succ.id - me.id);

  vector<CHID>::iterator it;
  CHID finger;
  unsigned int pos;
  for (unsigned int i = i0; i < NBCHID; i++) {
    finger = ConsistentHash::successorID(me.id,i);
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

  return true;
}

void
ChordFinger::init_state(vector<IDMap> ids)
{
  Chord::init_state(ids);
  IDMap succ = loctable->succ(me.id + 1);
  unsigned int i0 = log_2(succ.id - me.id);

  vector<IDMap>::iterator it;
  IDMap finger;
  unsigned int pos;
  for (unsigned int i = i0; i < NBCHID; i++) {
    finger.id = ConsistentHash::successorID(me.id,i);
    it = upper_bound(ids.begin(), ids.end(), finger, IDMap::cmp);
    pos = it - ids.begin();
    if (pos >= ids.size()) {
      pos = 0;
    }
    loctable->add_node(ids[pos]);
  }
}

void
ChordFinger::dump()
{
  Chord::dump();
  IDMap succ = loctable->succ(me.id + 1);
  unsigned int i0 = log_2(succ.id - me.id);
  CHID finger;
  for (unsigned int i = i0; i < NBCHID; i++) {
    finger = ConsistentHash::successorID(me.id,i);
     succ = loctable->succ(finger);
     if (succ.ip > 0) 
       printf("%qx: finger: %d : %qx%s : succ %qx%s\n", me.id, i, finger, PAD, succ.id, PAD);
  }
}
