/*
 * Copyright (c) 2003-2005 Jinyang Li
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef __CHORDFINGERPNS_H
#define __CHORDFINGERPNS_H


/* ChordFingerPNS does Gummadi^2's PNS proximity routing, it's completely static now*/

#include "chord.h"
#include <algorithm>
using namespace std;

#define USE_OVERLAP 0

class ChordFingerPNS: public Chord {
  public:
    ChordFingerPNS(IPAddress i, Args& a, LocTable *l = NULL, const char *name=NULL);
    ~ChordFingerPNS() {};
    string proto_name() { return "ChordFingerPNS"; }

    bool stabilized(vector<CHID> lid);
    void dump();
    void initstate();
    void oracle_node_joined(IDMap n);
    void oracle_node_died(IDMap n);

    void reschedule_pns_stabilizer(void *x);
    void fix_pns_fingers(bool restart);
    void join(Args*);

    void learn_info(IDMap n);
    bool replace_node(IDMap n, IDMap &replacement);
    uint num_live_samples(vector<IDMap> ss);

  protected:
    uint _base;
    uint _fingerlets;
    int _samples;
    uint _stab_pns_outstanding;
    uint _stab_pns_timer;
    bool _stab_pns_running;
};

#endif
