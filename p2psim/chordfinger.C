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
    loctable->pin(ConsistentHash::successorID(me.id, i));
  }
}

void
ChordFinger::fix_fingers()
{
  int i0 = 0;
  IDMap succ = loctable->succ(me.id + 1);

  if (succ.ip == 0 || succ.id == me.id) return;

  CHID gap = succ.id - me.id;
  while (gap > 0) {
    i0++;
    gap = gap >> 1;
  }
  
  vector<Chord::IDMap> v;
  for (unsigned int i = i0; i < NBCHID; i++) {
    v = find_successors(ConsistentHash::successorID(me.id,i), 1, true);
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
    if (lid[pos] != succ.id)
      return false;
  }

  return true;
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
