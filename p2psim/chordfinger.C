#include  "chordfinger.h"
#include <stdio.h>
#include <iostream>
#include <algorithm>
using namespace std;

ChordFinger::ChordFinger(Node *n) : Chord(n) 
{
  loctable->resize((1 + NBCHID + CHORD_SUCC_NUM), CHORD_SUCC_NUM);
  for (unsigned int i = 0; i < NBCHID; i++) {
    loctable->pin(ConsistentHash::successorID(me.id, i));
  }
}

void
ChordFinger::fix_fingers()
{
  int i0 = 0;
  IDMap succ = loctable->succ(1);

  if (succ.ip == 0 || succ.id == me.id) return;

  CHID gap = succ.id - me.id;
  while (gap > 0) {
    i0++;
    gap = gap >> 1;
  }
  
  vector<Chord::IDMap> v;
  for (unsigned int i = i0; i < NBCHID; i++) {
    v = find_successors(ConsistentHash::successorID(me.id,i), 1); 
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

  int i0 = 0;
  IDMap succ = loctable->succ(1);
  CHID gap = succ.id - me.id;
  while (gap > 0) {
    i0++;
    gap = gap >> 1;
  }

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
    if (lid[pos] != loctable->succID(finger))
      return false;
  }

  return true;
}
