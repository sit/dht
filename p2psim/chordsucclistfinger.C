#include  "chordsucclistfinger.h"
#include <iostream>
using namespace std;

ChordSuccListFinger::ChordSuccListFinger(Node *n) : ChordSuccList(n) 
{
  loctable->resize((1 + NBCHID + SUCC_NUM), SUCC_NUM);
  for (int i = 0; i < NBCHID; i++) {
    loctable->pin(ConsistentHash::successorID(me.id, i));
  }

}

void
ChordSuccListFinger::fix_fingers()
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
ChordSuccListFinger::stabilize()
{/*
  super.stabilize();
  fix_fingers();
  */
}
