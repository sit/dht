/*
 * Copyright (c) 2003 [NAMES_GO_HERE]
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

class LocTablePNS : public LocTable {
  public:
    LocTablePNS() : LocTable() {
    };
    ~LocTablePNS() {};

    class pns_entry{
      public:
      Chord::IDMap n;
      Chord::CHID f_s;
      Chord::CHID f_e;
      static bool cmp(const pns_entry& a, const pns_entry& b) {return a.n.id <= b.n.id;}
    };

    Chord::IDMap next_hop(Chord::CHID key, uint m, uint nsucc) {
      //implement SOSP'03's case 1
      //if my successor list includes less than m successors for key, 
      //then pick the physically closest successor to be the next_hop
      assert(nsucc == 1); //XXX i have not implemented multiple next hops for this case
      if (nsucc > 1) {
	uint num = 0;
	Time min_lat = 100000000;
	Time lat;
	Chord::IDMap min_s = me;
	Topology *t = Network::Instance()->gettopology();
	vector<Chord::IDMap> succs = this->succs(me.id+1, nsucc);
	bool seen_succ = false;
	for (int i = ((int)succs.size()) - 1; i >= 0; i--) {
	  if (ConsistentHash::betweenrightincl(me.id, succs[i].id,key)) {
	    seen_succ = true;
	  } 
	  if (seen_succ) {
	    if (ConsistentHash::betweenrightincl(me.id,succs[i].id,key)) {
	      if (!USE_OVERLAP) continue;
	    }else {
	      if (num > (nsucc - m)) goto DDONE;
	      num++;
	    }
	    lat = t->latency(me.ip, succs[i].ip);
	    if (min_lat > lat) {
	      min_lat = lat;
	      min_s = succs[i];
	    }
	  }
	}
DDONE:
	if (seen_succ) {
	  printf("%u,%qx shortcut query %qx to node %u,%qx (succ sz %d)\n", me.ip, me.id, key, min_s.ip, min_s.id,succs.size());
	  assert(min_s.ip != me.ip);
	  return min_s;
	}
      } 
      return LocTable::next_hop(key);
    };
};

class ChordFingerPNS: public Chord {
  public:
    ChordFingerPNS(IPAddress i, Args& a, LocTable *l = NULL);
    ~ChordFingerPNS() {};
    string proto_name() { return "ChordFingerPNS"; }

    bool stabilized(vector<CHID> lid);
    void dump();
    void initstate();

    void reschedule_pns_stabilizer(void *x);
    void fix_pns_fingers(bool restart);
    //void my_next_recurs_handler(next_recurs_args *, next_recurs_ret *);
    //void pns_next_recurs_handler(next_recurs_args *, next_recurs_ret *);
    void join(Args*);

  protected:
    uint _base;
    int _samples;
    uint _stab_pns_outstanding;
    uint _stab_pns_timer;
    bool _stab_pns_running;
};

#endif
