#ifndef __CHORDSUCCLISTFINGER_H
#define __CHORDSUCCLISTFINGER_H

#include "chord.h"

/* ChordFinger implements finger table in addition to Chord with succ list*/

class ChordFinger: public Chord {
  public:
    ChordFinger(Node *n, uint base, uint successors, uint maxf, LocTable *l = NULL);
    ~ChordFinger() {};
    string proto_name() { return "ChordFinger"; }

    void stabilize();
    bool stabilized(vector<CHID> lid);
    void reschedule_stabilizer(void *x);
    void dump();
    void init_state(vector<IDMap> ids);

  protected:
    void fix_fingers();
    uint _base;
    uint _maxf;
    uint _numf; //number of fingers ChordFinger should be keeping
};

#endif
