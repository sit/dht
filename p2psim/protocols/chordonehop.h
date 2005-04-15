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

#ifndef __CHORDONEHOP_H
#define __CHORDONEHOP_H

#include "chord.h"

class ChordOneHop: public Chord {
  public:
    ChordOneHop(IPAddress i, Args& a);
    ~ChordOneHop();
    string proto_name() { return "ChordOneHop";}

    struct deladd_args {
      IDMap n;
    };

    void join(Args *);
    void stabilize();
    bool stabilized(vector<ConsistentHash::CHID> lid) {return false;};
    void reschedule_basic_stabilizer(void *x);
    void init_state(vector<IDMap> ids);

    void del_handler(deladd_args *, void *);
    void add_handler(deladd_args *, void *);
    void getloctable_handler(void *, get_predsucc_ret *);

    virtual vector<IDMap> find_successors(CHID key, uint m, uint all, bool is_lookup);
};
#endif //__CHORDONEHOP_H

