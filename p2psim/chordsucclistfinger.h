#ifndef __CHORDFINGER_H
#define __CHORDFINGER_H

#include "chord.h"

/* ChordSuccListFinger implements finger table in addition to ChordSuccList*/

class ChordSuccListFinger: public ChordSuccList {
  public:
    ChordSuccListFinger(Node *n) : Chord(n) {};
    ~ChordSuccListFinger() {};

  protected:
    void fix_fingers();
    void stabilize();
};

#endif
