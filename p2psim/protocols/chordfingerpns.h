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

#define USE_OVERLAP 1

class LocTablePNS : public LocTable {
  public:
    LocTablePNS() : LocTable() {
    };
    ~LocTablePNS() {};

    Chord::IDMap next_hop(Chord::CHID key, bool *done, uint m, uint nsucc) {
      //implement SOSP'03's case 1
      //if my successor list includes less than m successors for key, 
      //then pick the physically closest successor to be the next_hop
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
	if (done) {
	  *done = false;
	}
	if (seen_succ) {
	  printf("%u,%qx shortcut query %qx to node %u,%qx (succ sz %d)\n", me.ip, me.id, key, min_s.ip, min_s.id,succs.size());
	  assert(min_s.ip != me.ip);
	  return min_s;
	}
      } 
      return LocTable::next_hop(key, done);
    };
};

class ChordFingerPNS: public Chord {
  public:
    ChordFingerPNS(Node *n, Args& a, LocTable *l = NULL);
    ~ChordFingerPNS() {};
    string proto_name() { return "ChordFingerPNS"; }

    struct pns_next_recurs_args {
	bool is_lookup;
	CHID key;
	vector<IDMap> path;
	uint m;
	uint overlap;
      };


    struct pns_next_recurs_ret {
      vector< pair<IDMap,IDMap> > v;
      vector<IDMap> path;
      uint overlap;
    };

    bool stabilized(vector<CHID> lid);
    void dump();
    void init_state(vector<IDMap> ids);

    vector<Chord::IDMap> find_successors_recurs(CHID key, uint m, bool is_lookup, uint *recurs_int);
    vector<Chord::IDMap> find_successors(CHID key, uint m, bool is_lookup);
    void ChordFingerPNS::pns_next_recurs_handler(pns_next_recurs_args *, pns_next_recurs_ret *);

  protected:
    uint _base;
    int _samples;
};

#endif
