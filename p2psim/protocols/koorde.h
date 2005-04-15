/*
 * Copyright (c) 2003-2005 [Frans Kaashoek]
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

#ifndef __KOORDE_H
#define __KOORDE_H

#include "chord.h"

// Koorde extends base Chord with k-degree debruijn routing
class Koorde : public Chord {
public:
  Koorde(IPAddress, Args& a);
  ~Koorde() {};
  string proto_name() { return "Koorde"; }

  struct koorde_lookup_arg {
    CHID k;
    CHID kshift;
    CHID i;
    uint nsucc;
    IDMap dead;
  };

  struct koorde_lookup_ret {
    IDMap next;
    CHID k;
    CHID kshift;
    CHID i;
    bool done;
    vector<IDMap> v;
  };

  // RPC handlers
  void koorde_next (koorde_lookup_arg *, koorde_lookup_ret *);

  bool stabilized(vector<ConsistentHash::CHID>);
  void join(Args*);
  vector<Chord::IDMap> find_successors(CHID key, uint m, uint type, 
      IDMap *lasthop = NULL, lookup_args *a = NULL);
  vector<Chord::IDMap> find_successors_recurs(CHID key, uint m, uint type, 
      IDMap *lasthop=NULL, lookup_args *a=NULL) { exit(1);}
  void initstate();
  void dump();

protected:
  uint logbase;  // log degree
  uint k;  // k-degree de bruijn graph; 
  uint nsucc;
  uint fingers;   // number of successors of debruijn finger
  uint resilience;  // resilience
  uint _stab_debruijn_outstanding;
  uint _stab_debruijn_running;
  uint _stab_debruijn_timer;

  Chord::CHID debruijn;  // = k * me
  Chord::CHID debruijnpred;  // = k * me - x

  bool isstable;
  vector<IDMap> lastdfingers;
  
  Chord::CHID Koorde::nextimagin (CHID i, CHID kshift);
  Chord::CHID Koorde::firstimagin (CHID, CHID, CHID, CHID*);
  IDMap Koorde::closestpreddfinger (CHID);

  void fix_debruijn();
  void reschedule_debruijn_stabilizer(void *x);
  bool debruijn_stabilized (ConsistentHash::CHID finger, uint n,
			    vector<ConsistentHash::CHID> lid);
  void debruijn_dump (Chord::CHID finger, uint n);

};

#endif // __KOORDE_H
