#ifndef __CHORDSUCCLISTFINGER_H
#define __CHORDSUCCLISTFINGER_H

#include "chord.h"

/* ChordFinger implements finger table in addition to Chord with succ list*/

class ChordFinger: public Chord {
  public:
    ChordFinger(Node *n);
    ~ChordFinger() {};

  protected:
    void fix_fingers();
    void stabilize();
};

#endif
