#include  "chordfinger.h"
#include <iostream>
using namespace std;

ChordFinger::ChordFinger(Node *n) : Chord(n) 
{
  loctable->resize((1 + NBCHID + CHORD_SUCC_NUM), CHORD_SUCC_NUM);
  for (int i = 0; i < NBCHID; i++) {
    loctable->pin(ConsistentHash::successorID(me.id, i));
  }

}

void
ChordFinger::fix_fingers()
{
  int i0 = 1;
  IDMap succ = loctable->succ(1);
  CHID gap;
  if (succ.id > me.id) {
    gap = succ.id - me.id;
  }else{
    gap = me.id - succ.id;
  }
  while (gap > 0) {
    i0++;
    gap = gap >> 1;
  }
  vector<Chord::IDMap> v;
  for (int i = i0; i < NBCHID; i++) {
    v = find_successors(ConsistentHash::successorID(me.id,i), 1); 
    if (v.size() > 0) loctable->add_node(v[0]);
  }
}

void
ChordFinger::stabilize()
{/*
  super.stabilize();
  fix_fingers();
  */
}
