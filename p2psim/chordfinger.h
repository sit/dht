#ifndef __CHORDSUCCLISTFINGER_H
#define __CHORDSUCCLISTFINGER_H

#include "chord.h"

/* ChordFinger implements finger table in addition to Chord with succ list*/

class ChordFinger: public Chord {
  public:
    ChordFinger(Node *n, uint base, uint successors);
    ~ChordFinger() {};

    void stabilize();
    bool stabilized(vector<CHID> lid);
    void reschedule_stabilizer(void *x);
    void dump();

  protected:
    void fix_fingers();
    uint _base;
};

#endif
