#ifndef __CHORDFINGERPNS_H
#define __CHORDFINGERPNS_H


/* ChordFingerPNS does Gummadi^2's PNS proximity routing, it's completely static now*/

#include "chord.h"

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
	latency_t min_lat = 100000000;
	latency_t lat;
	Chord::IDMap min_s = me;
	Topology *t = Network::Instance()->gettopology();
	vector<Chord::IDMap> succs = this->succs(me.id+1, nsucc);
	bool seen_succ = false;
	for (int i = ((int)succs.size()) - 1; i >= 0; i--) {
	  assert(i >= 0 && i < (int)succs.size());
	  if (ConsistentHash::betweenrightincl(me.id, succs[i].id,key)) {
	    seen_succ = true;
	  } else if (seen_succ) {
	    if (num <= (nsucc-m)) {
	      lat = t->latency(me.ip, succs[i].ip);
	      if (min_lat > lat) {
		min_lat = lat;
		min_s = succs[i];
	      }
	    }else{
	      break;
	    }
	    num++;
	  }
	}
	*done = false;
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

    struct pns_next_recurs_ret {
      vector< pair<IDMap,IDMap> > v;
      vector<IDMap> path;
    };

    bool stabilized(vector<CHID> lid);
    void dump();
    void init_state(vector<IDMap> ids);

    vector<Chord::IDMap> find_successors_recurs(CHID key, uint m, bool is_lookup, uint *recurs_int);
    void ChordFingerPNS::pns_next_recurs_handler(next_recurs_args *, pns_next_recurs_ret *);

  protected:
    uint _base;
    int _samples;
};

#endif
