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

#ifndef __CHORDSUCCLISTFINGER_H
#define __CHORDSUCCLISTFINGER_H


/* ChordFinger implements finger table in addition to Chord with succ list*/
#include "chord.h"

class ChordFinger: public Chord {
  public:
    ChordFinger(IPAddress i, Args& a, LocTable *l = NULL);
    ~ChordFinger() {};
    string proto_name() { return "ChordFinger"; }

    bool stabilized(vector<CHID> lid);
    void reschedule_finger_stabilizer(void *x);
    void dump();
    void initstate();
    void oracle_node_joined(IDMap n);
    void oracle_node_died(IDMap n);
    virtual void join(Args *);

  protected:
    uint _fingerlets;
    uint _stab_finger_timer;
    void fix_fingers(bool restart=false);
    uint _base;
    uint _maxf;
    uint _numf; //number of fingers ChordFinger should be keeping
    uint _stab_finger_outstanding;
    bool _stab_finger_running;

};

#endif
