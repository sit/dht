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

    void rebuild_pns_finger_table(uint base, uint nsucc) {

      vector<Chord::IDMap> scs = succs(me.id + 1, nsucc);
      Chord::CHID min;
      if (scs.size()) 
	min = scs[scs.size()-1].id - me.id;
      else
	min = (Chord::CHID)-1;

      Topology *t = Network::Instance()->gettopology();

      Chord::CHID lap = (Chord::CHID) -1;
      Chord::CHID finger;
      Time tt = now();

      pns_entry f;
      if (min < min_lap) {
	fingers.clear();
	while (lap > min_lap) {
	  lap = lap/base;
	  for (uint j = 1; j <= (base-1); j++) {
	    finger = lap * j + me.id;
	    f.n.id = finger;
	    f.f_s = finger;
	    f.f_e = finger + lap;
	    fingers.push_back(f);
	  }
	}
	sort(fingers.begin(), fingers.end(), pns_entry::cmp);
      }

      for (uint i = 0; i < fingers.size(); i++) {
	idmapwrap *elm = ring.closestsucc(fingers[i].f_s);
	assert(elm);
	uint min_l = 1000000;
	Chord::IDMap min_f;
	while (ConsistentHash::between(fingers[i].f_s, fingers[i].f_e, elm->id)) {
	  if (((!_timeout || (tt - elm->timestamp) < _timeout)) && (elm->n.ip!= me.ip) && (t->latency(me.ip, elm->n.ip)) < min_l) {
	    min_f = elm->n;
	  }
	  elm = ring.next(elm);
	  if (!elm) elm = ring.first();
	}
	if (min_l < 1000000) 
	  fingers[i].n = min_f;
	else
	  fingers[i].n.ip = 0;
      }
    }

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
      return LocTable::next_hop(key,done);
/*
      uint fsz = fingers.size();
      if (fsz == 0) {
	return LocTable::next_hop(key,done);
      }else{
	//only use PNS fingers + succ as next hops
	pns_entry tmp;
	tmp.n.id = key;
	uint pos = upper_bound(fingers.begin(), fingers.end(), tmp, pns_entry::cmp) - fingers.begin();
	pos = pos % fsz;
	uint i = pos;
	while (fingers[i].n.ip == 0) {
	  if (i == 0) 
	    i = fsz-1;
	  else
	    i--;
	  if (i == pos) 
	    return LocTable::next_hop(key,done);
	}
	if (ConsistentHash::between(me.id, key, fingers[i].n.id))
	  return fingers[i].n;
	else
	  return LocTable::next_hop(key,done);
      }
      */
    };

    void del_node(Chord::IDMap n) {

      LocTable::del_node(n);
      pns_entry f;
      f.n = n;
      uint i = upper_bound(fingers.begin(), fingers.end(), f, pns_entry::cmp) - fingers.begin();

      if (i >= fingers.size()) return;

      if (fingers[i].n.ip == n.ip) {

	Time tt = now();
	Topology *t = Network::Instance()->gettopology();
	//replace this pns node with another valid one if possible
	idmapwrap *elm = ring.closestsucc(fingers[i].f_s);
	uint min_l = 1000000;
	Chord::IDMap min_f;
	while (ConsistentHash::between(fingers[i].f_s, fingers[i].f_e, elm->id)) {
	  if (((!_timeout || (tt - elm->timestamp) < _timeout)) && ((t->latency(me.ip, elm->n.ip)) < min_l)) {
	    min_f = elm->n;
	  }
	  elm = ring.next(elm);
	}
	if (min_l < 1000000) 
	  fingers[i].n = min_f;
	else
	  fingers[i].n.ip = 0;
      }
    };

    Chord::CHID min_lap;

    vector<pns_entry> fingers;
};

class ChordFingerPNS: public Chord {
  public:
    ChordFingerPNS(Node *n, Args& a, LocTable *l = NULL);
    ~ChordFingerPNS() {};
    string proto_name() { return "ChordFingerPNS"; }

    bool stabilized(vector<CHID> lid);
    void dump();
    void init_state(vector<IDMap> ids);

    void reschedule_pns_stabilizer(void *x);
    void fix_pns_fingers(bool restart);
    void my_next_recurs_handler(next_recurs_args *, next_recurs_ret *);
    void pns_next_recurs_handler(next_recurs_args *, next_recurs_ret *);
    void join(Args*);

  protected:
    uint _base;
    int _samples;
    uint _stab_pns_outstanding;
    bool _stab_pns_running;
};

#endif
