#ifndef __CHORDFINGERPNS_H
#define __CHORDFINGERPNS_H

#include "chord.h"

/* ChordFingerPNS does Gummadi^2's PNS proximity routing, it's completely static now*/

class ChordFingerPNS: public Chord {
  public:
    ChordFingerPNS(Node *n, uint base, uint successors, int samples = 64);
    ~ChordFingerPNS() {};
    string proto_name() { return "ChordFingerPNS"; }

    bool stabilized(vector<CHID> lid);
    void dump();
    void init_state(vector<IDMap> ids);

  protected:
    uint _base;
    int _samples;
};

#endif
