#ifndef __CHORDSUCCLISTFINGER_H
#define __CHORDSUCCLISTFINGER_H

#include "chordsucclist.h"

/* ChordSuccListFinger implements finger table in addition to ChordSuccList*/

class ChordSuccListFinger: public ChordSuccList {
  public:
    ChordSuccListFinger(Node *n);
    ~ChordSuccListFinger() {};

  protected:
    void fix_fingers();
    void stabilize();
};

#endif
