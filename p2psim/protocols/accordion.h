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

#ifndef __ACCORDION_H
#define __ACCORDION_H

#include "p2psim/bighashmap.hh"
#include "chord.h"
class RateControlQueue;


typedef Chord::IDMap IDMap;
class Accordion: public P2Protocol {
  public:
    Accordion(IPAddress i, Args& a);
    ~Accordion();
    string proto_name() { return "Accordion";}
    IDMap idmap() { return _me;}
    ConsistentHash::CHID id() { return _me.id;}

    struct lookup_args{
      ConsistentHash::CHID key;
      uint hops;
      Time to_lat;
      uint to_num;
      uint learnsz;
      bool no_drop;
      IDMap nexthop;
      IDMap to;
      IDMap from;
      IDMap src;
      uint m;
      IDMap ori;
      uint parallelism;
      uint type;
      ConsistentHash::CHID overshoot;
      Time timeout;
      IDMap prevhop;
      vector<IDMap> deadnodes;
    };

    struct lookup_ret{
      vector<IDMap> v;
      bool is_succ;
      bool done;
    };

    struct alert_args {
      vector<IDMap> v;
      vector<IDMap> d;
      ConsistentHash::CHID k;
      IDMap src;
    };

    struct find_successors_ret{
      vector<IDMap> v;
    };

    struct get_predsucc_args {
      IDMap src;
      IDMap n;
      uint m;
    };

    struct get_predsucc_ret {
      vector<IDMap> v;
      IDMap pred;
    };

    struct notify_succdeath_ret {
      IDMap succ;
    };

    struct learn_args {
      IDMap src;
      IDMap n;
      int m;
      IDMap start;
      IDMap end;
      double timeout;
    };

    struct learn_ret {
      int stat;
      bool is_succ;
      vector<IDMap> v;
    };

    void find_successors_handler(lookup_args *, lookup_ret *);
    void get_predsucc_handler(get_predsucc_args *, get_predsucc_ret *);
    int learn_cb(bool, learn_args *, learn_ret *);
    int fix_succ_cb(bool, get_predsucc_args *, get_predsucc_ret *);
    int fix_pred_cb(bool, get_predsucc_args *, get_predsucc_ret *);
    int null_cb(bool, lookup_args *a, lookup_ret *r);
    void learn_handler(learn_args *la, learn_ret *lr);
    void join_handler(lookup_args*, lookup_ret *);
    void join_learn();
    void donelookup_handler(lookup_args *, lookup_ret *);
    void join(Args *);
    void lookup(Args *);
    void nodeevent (Args *) {};
    void crash(Args*);
    void initstate();
    void stab_succ(void *x);

    void next_recurs(lookup_args *, lookup_ret *);
    int next_recurs_cb(bool, lookup_args *, lookup_ret *);

    void next_iter(lookup_args *, lookup_ret *);
    void next(lookup_args *,lookup_ret *);
    int next_iter_cb(bool,lookup_args *,lookup_ret *);
    void alert_nodes(alert_args *la, lookup_ret *lr);
    void alert_lookup_nodes(ConsistentHash::CHID key, Time to);
    int alert_cb(bool b, alert_args *la, lookup_ret *lr);

    static void empty_cb(void *x);
    bool check_pred_correctness(ConsistentHash::CHID, IDMap n);
    void empty_queue(void *a);

    void fix_succ(void *a);
    void fix_pred(void *a);

    static string printID(ConsistentHash::CHID id);
    static string print_succs(vector<IDMap> v);

    uint budget() { return _bw_overhead;}

    static vector<IDMap> ids;
    static bool sorted;
    static vector<double> sort_live;
    static vector<double> sort_dead;
    static vector<double> min_bw;

    // to allow RateControlQueue to access protected members of Node
    template<class BT, class AT>
      void delaycb(int d, void (BT::*fn)(AT), AT args, BT *target = NULL) {
        P2Protocol::delaycb(d, fn, args, target);
      }

    // to allow RateControlQueue to access protected members of Node
    bool _doRPC(IPAddress dst, void (*fn)(void *), void *args, Time timeout) {
      P2Protocol::_doRPC(dst, fn, args, timeout);
    }

  protected:
    IDMap _me;
    RateControlQueue *_rate_queue;
    LocTable *loctable;
    unsigned PKT_SZ(unsigned ids, unsigned others);

  private:
    struct Stat {
      bool alive;
      double ti;
    };
    Time _join_scheduled;
    uint _burst_sz;
    uint _bw_overhead;
    bool _recurs;
    uint _stab_basic_timer;
    Time _last_joined_time;
    bool _stab_basic_running;

    double _fixed_stab_to;
    double _fixed_lookup_to;
    Time _fixed_stab_int;

    Time _last_stab;
    int _parallelism;
    int _max_p;
    Time _next_adjust;
    Time _adjust_interval;
    uint _lookup_times;
    uint _empty_times;
    uint _nsucc;
    IDMap _wkn;
    uint _to_multiplier;
    uint _learn_num;
    ConsistentHash::CHID _max_succ_gap;
    double _tt;
    vector<Stat> _stat;
    static vector<uint> rtable_sz;
    unsigned long _last_bytes;
    Time _last_bytes_time;
    vector<double> _calculated_prob;
    Time _last_calculated;
    uint _est_n;
    
    HashMap<ConsistentHash::CHID, Time> _outstanding_lookups;
    HashMap<ConsistentHash::CHID, Time> _forwarded;
    HashMap<ConsistentHash::CHID, uint> _forwarded_nodrop;
    HashMap<ConsistentHash::CHID, list<IDMap>* > _sent;
    HashMap<ConsistentHash::CHID, list<IDMap>* > _dead;
    HashMap<ConsistentHash::CHID, ConsistentHash::CHID> _progress;

    void consolidate_succ_list(IDMap n, vector<IDMap> oldlist, vector<IDMap> newlist, bool is_succ = true);
    void adjust_parallelism();
    void add_stat(double ti, bool live);
    double est_timeout(double p);
    void adjust_timeout();

    Topology *_top; //i hate obtaining topology pointer every time
};
#endif

