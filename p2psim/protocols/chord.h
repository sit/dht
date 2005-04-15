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

#ifndef __CHORD_H
#define __CHORD_H

#include "p2psim/p2protocol.h"
#include "consistenthash.h"
#include "p2psim/network.h"

#define TIMEOUT_RETRY 5

//#define RECORD_FETCH_LATENCY

#define TIMEOUT(src,dst) (Network::Instance()->gettopology()->latency(src,dst)<=1000)?_to_multiplier*2*Network::Instance()->gettopology()->latency(src,dst):1000

#define PKT_OVERHEAD 20

#define TYPE_USER_LOOKUP 0
#define TYPE_JOIN_LOOKUP 1
#define TYPE_FINGER_LOOKUP 2
#define TYPE_FIXSUCC_UP 3
#define TYPE_FIXSUCCLIST_UP 4
#define TYPE_FIXPRED_UP 5
#define TYPE_FINGER_UP 6
#define TYPE_PNS_UP 7
#define TYPE_MISC 8

#define MIN_BASIC_TIMER 100

class LocTable;

class Chord : public P2Protocol {
public:
  
  typedef ConsistentHash::CHID CHID;

  class IDMap{
    public:
    ConsistentHash::CHID id; //consistent hashing ID for the node
    IPAddress ip; //the IP address for the node
    Time timestamp; //some kind of heartbeat sequence number
    Time alivetime; //how long this node has stayed alive
    static bool cmp(const IDMap& a, const IDMap& b) { return (a.id <= b.id);}
    bool operator==(const IDMap a) { return (a.id == id); }
  };

  Chord(IPAddress i,  Args& a, LocTable *l = NULL, const char *name=NULL); 
  virtual ~Chord();
  virtual string proto_name() { return "Chord"; }

  // Functions callable from events file.
  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void lookup(Args*);
  virtual void insert(Args*) {};
  virtual void nodeevent (Args *) {};

  struct get_predsucc_args {
    bool pred; //need to get predecessor?
    int m; //number of successors wanted 0
  };
  struct get_predsucc_ret {
    vector<IDMap> v;
    IDMap dst; 
    IDMap n;
  };

  struct notify_args {
    IDMap me;
  };
  struct notify_ret {
    int dummy;
  };

  struct alert_args {
    IPAddress dst;
    IDMap n;
  };

  struct hop_info {
    IDMap from;
    IDMap to;
    uint hop;
  };

  
  struct next_args {
    IDMap src;
    CHID key;
    uint m;
    uint alpha; //get m out of the first all successors
    bool retry;
    uint type;
    vector<IDMap> deadnodes;
  };

  struct next_ret {
    bool done;
    vector<IDMap> v;
    vector<IDMap> next;
    bool correct;
    IDMap dst;
    IDMap lastnode;
  };

  struct nextretinfo{
    hop_info link;
    next_ret ret;
    bool free;
  };

  struct find_successors_args {
    IDMap src;
    CHID key;
    uint m;
    uint all;
  };

  struct find_successors_ret {
    vector<IDMap> v;
    IDMap last;
    IDMap dst;
  };

  struct lookup_path {
    IDMap n;
    bool tout;
  };

  struct next_recurs_args {
    uint type;
    CHID key;
    IPAddress ipkey;
    uint m;
    IDMap src;
  };

  struct next_recurs_ret {
    vector<IDMap> v;
    vector<lookup_path> path;
    bool correct;
    uint finish_time;
    IDMap lasthop;
    IDMap prevhop;
    IDMap nexthop;
  };

  struct lookup_args{
    CHID key;
    IPAddress ipkey;
    Time start;
    Time latency;
    uint retrytimes;
    Time total_to;
    uint num_to;
    uint hops;
  };
  // RPC handlers.
  void null_handler (void *args, IDMap *ret);
  void get_predsucc_handler(get_predsucc_args *, get_predsucc_ret *);
  void notify_handler(notify_args *, notify_ret *);
  void alert_handler(alert_args *, void *);
  void next_handler(next_args *, next_ret *);
  void find_successors_handler(find_successors_args *, 
                               find_successors_ret *);

  void final_recurs_hop(next_recurs_args *args, next_recurs_ret *ret);
  void next_recurs_handler(next_recurs_args *, next_recurs_ret *);
  void lookup_internal(lookup_args *a);
  void alert_delete(alert_args *aa);

  CHID id() { return me.id; }
  IDMap idmap() { return me;}
  virtual void initstate();
  virtual bool stabilized(vector<CHID>);
  bool check_correctness(CHID k, vector<IDMap> v);
  virtual void oracle_node_died(IDMap n);
  virtual void oracle_node_joined(IDMap n);
  void add_edge(int *matrix, int sz);

  virtual void dump();

  IDMap next_hop(CHID k);

  char *ts();
  string header(); //debug message header
  static string printID(CHID id);

  void stabilize();
  virtual void reschedule_basic_stabilizer(void *);

  bool inited() {return _inited;};
  char *print_path(vector<lookup_path> &p, char *tmp);

protected:
  //chord parameters
  uint _nsucc;
  uint _allfrag;
  uint _timeout;
  uint _to_multiplier;
  bool _stab_basic_running;
  uint _stab_basic_timer;
  uint _stab_succlist_timer;
  uint _stab_basic_outstanding;
  uint _max_lookup_time;
  uint _frag;
  uint _alpha;
  int _asap;
  uint _recurs;
  uint _recurs_direct;
  uint _stopearly_overshoot;
  IDMap _wkn;
  uint _join_scheduled;
  uint _parallel;
  uint _learn;
  uint _ipkey;
  uint _last_succlist_stabilized;
  uint _random_id;

  LocTable *loctable;
  LocTable *learntable;
  IDMap me; 
  CHID _prev_succ;
  uint i0;
  vector<IDMap> lastscs;
  bool _isstable;
  bool _inited;

#ifdef RECORD_FETCH_LATENCY
  static vector<uint> _fetch_lat;
#endif

  virtual vector<IDMap> find_successors_recurs(CHID key, uint m, uint type, IDMap *lasthop = NULL, lookup_args *a = NULL);
  virtual vector<IDMap> find_successors(CHID key, uint m, uint type, IDMap *lasthop = NULL, lookup_args *a = NULL);
  virtual void learn_info(IDMap n);
  virtual bool replace_node(IDMap n, IDMap &replacement);

  template<class BT, class AT, class RT>
    bool Chord::failure_detect(IDMap dst, void (BT::* fn)(AT *, RT *), AT *args, RT *ret, 
	uint type, uint num_args_id = 0, uint num_args_else = 0, int num_retry=TIMEOUT_RETRY);

  void fix_successor(void *x=NULL);
  void fix_predecessor();
  void fix_successor_list();
  void check_static_init();
  void record_stat(IPAddress src, IPAddress dst, uint type, uint num_ids, uint num_else = 0);
  void record_lookupstat(uint num, uint type);

private:
  Time _last_join_time;
  static vector<uint> rtable_sz;

};

#define LOC_REPLACEMENT 0
#define LOC_HEALTHY 1
#define LOC_ONCHECK 2
#define LOC_DEAD 6

class LocTable {

  public:
    struct idmapwrap {
	Chord::IDMap n;
	Chord::CHID id;
	sklist_entry<idmapwrap> sortlink_;
	bool is_succ;
	int status;
	Chord::CHID fs;
	Chord::CHID fe;
	ConsistentHash::CHID follower;
	idmapwrap(Chord::IDMap x) {
	  n = x;
	  id = x.id;
	  status = 0;
	  fs = fe = 0;
	  is_succ = false;
	  follower = 0;
	}
    };

    struct idmapcompare{
      idmapcompare() {}
      int operator() (ConsistentHash::CHID a, ConsistentHash::CHID b) const
      { if (a == b) 
	  return 0;
	else if (a < b)
	  return -1;
	else 
	  return 1;
      }
    };


  LocTable();
  void init (Chord::IDMap me);
  virtual ~LocTable();

    idmapwrap *get_naked_node(ConsistentHash::CHID id);
    Chord::IDMap succ(ConsistentHash::CHID id, int status = LOC_HEALTHY);
    vector<Chord::IDMap> succs(ConsistentHash::CHID id, unsigned int m, int status = LOC_HEALTHY);
    vector<Chord::IDMap> preds(Chord::CHID id, uint m, int status = LOC_HEALTHY, double to=0.0);
    vector<Chord::IDMap> between(ConsistentHash::CHID start, ConsistentHash::CHID end, int status = LOC_HEALTHY);
    Chord::IDMap pred(Chord::CHID id, int status = LOC_ONCHECK);
    void checkpoint();
    void print();

    bool update_ifexists(Chord::IDMap n, bool replacement=false);
    bool add_node(Chord::IDMap n, bool is_succ=false, bool assertadd=false,Chord::CHID fs=0,Chord::CHID fe=0, bool replacement=false);
    int add_check(Chord::IDMap n);
    void add_sortednodes(vector<Chord::IDMap> l);
    bool del_node(Chord::IDMap n, bool force=false);
    virtual void del_all();
    void notify(Chord::IDMap n);
    uint size(uint status=LOC_HEALTHY, double to = 0.0);
    uint succ_size();
    void last_succ(Chord::IDMap n);
    uint live_size(double to = 0.0);
    bool is_succ(Chord::IDMap n);
    void set_evict(bool v) { _evict = v; }
    void set_timeout(uint to) {_timeout = to;}

    //pick the next hop for lookup;
    virtual vector<Chord::IDMap> next_hops(Chord::CHID key, uint nsz = 1);
    virtual Chord::IDMap next_hop(Chord::CHID key); 

    vector<Chord::IDMap> get_all(uint status=LOC_HEALTHY);
    Chord::IDMap first();
    Chord::IDMap last();
    Chord::IDMap search(ConsistentHash::CHID);
    int find_node(Chord::IDMap n);
    void dump();
    void stat();
    double pred_biggest_gap(Chord::IDMap &start, Chord::IDMap &end, Time stabtimer, double to = 0.0); //these two functions are too specialized
    uint sample_smallworld(uint est_n, Chord::IDMap &askwhom,Chord::IDMap &start, Chord::IDMap &end, double tt = 0.9, ConsistentHash::CHID maxgap = 0);
    void rand_sample(Chord::IDMap &askwhom, Chord::IDMap &start, Chord::IDMap &end);
    vector<Chord::IDMap> get_closest_in_gap(uint m, ConsistentHash::CHID start, ConsistentHash::CHID end, Chord::IDMap src, Time stabtime, double to);
    vector<Chord::IDMap> next_close_hops(ConsistentHash::CHID key, uint n, Chord::IDMap src, double to = 0.0);

  protected:
    bool _evict;
    skiplist<idmapwrap, ConsistentHash::CHID, &idmapwrap::id, &idmapwrap::sortlink_, idmapcompare> ring;
    Chord::IDMap me;
    uint _max;
    uint _timeout;
    ConsistentHash::CHID full;
    Time lastfull;

    //evict one node to make sure ring contains <= _max elements
    void evict(); 
};
#define CDEBUG(x) if(p2psim_verbose >= (x)) cout << header() 

#endif //__CHORD_H
