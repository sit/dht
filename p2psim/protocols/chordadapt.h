/*
 * Copyright (c) 2003 [Jinyang Li]
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

#ifndef __CHORDADAPT_H
#define __CHORDADAPT_H

#include "p2psim/bighashmap.hh"
#include "chord.h"
#include "ratecontrolqueue.h"

//#define PKT_SZ(ids,others) (PKT_OVERHEAD + 4 * ids + others)


typedef Chord::IDMap IDMap;
class ChordAdapt: public P2Protocol {
  public:
    ChordAdapt(IPAddress i, Args& a);
    ~ChordAdapt();
    string proto_name() { return "ChordAdapt";}
    IDMap idmap() { return _me;}
    ConsistentHash::CHID id() { return _me.id;}

    struct notify_info {
      IDMap n;
      uint ttl;
      bool dead;
    };

    struct lookup_args{
      ConsistentHash::CHID key;
      uint hops;
      Time to_lat;
      uint to_num;
      uint learnsz;
      bool no_drop;
      IDMap nexthop;
      IDMap from;
      IDMap src;
      uint m;
      IDMap ori;
      uint parallelism;
      uint type;
      ConsistentHash::CHID overshoot;
      Time timeout;
    };

    struct lookup_ret{
      vector<IDMap> v;
      bool is_succ;
      bool done;
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

    struct notify_succdeath_args {
      vector<notify_info> info;
      IDMap src;
      IDMap n;
    };

    struct notify_succdeath_ret {
      IDMap succ;
    };

    struct learn_args {
      IDMap src;
      IDMap n;
      int m;
      IDMap end;
      Time timeout;
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

    void notify_pred();
    void notify_succdeath_handler(notify_succdeath_args *, notify_succdeath_ret *);
    int notify_pred_cb(bool, notify_succdeath_args *, notify_succdeath_ret *);

    static void empty_cb(void *x);
    bool check_pred_correctness(ConsistentHash::CHID, IDMap n);
    void empty_queue(void *a);

    void fix_succ(void *a);
    void fix_pred(void *a);

    static string printID(ConsistentHash::CHID id);
    static string print_succs(vector<IDMap> v);

    static vector<IDMap> ids;
    static bool sorted;
    static vector<Time> sort_live;
    static vector<Time> sort_dead;

  protected:
    IDMap _me;
    RateControlQueue *_rate_queue;
    LocTable *loctable;
    unsigned PKT_SZ(unsigned ids, unsigned others);

  private:
    Time _join_scheduled;
    uint _burst_sz;
    uint _bw_overhead;
    uint _big_overhead;
    uint _stab_basic_timer;
    Time _last_joined_time;
    bool _stab_basic_running;
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
    vector<Time> _live_stat;
    vector<Time> _dead_stat;
    vector<Time> _calculated_prob;
    Time _last_calculated;
    
    HashMap<ConsistentHash::CHID, Time> _outstanding_lookups;
    HashMap<ConsistentHash::CHID, Time> _forwarded;
    HashMap<ConsistentHash::CHID, uint> _forwarded_nodrop;

    void consolidate_succ_list(IDMap n, vector<IDMap> oldlist, vector<IDMap> newlist, bool is_succ = true);
    void adjust_parallelism();
    void add_stat(Time t, bool live);
    Time est_timeout(double p);

    vector<notify_info> notifyinfo;
    Topology *_top; //i hate obtaining topology pointer every time
};
#endif

