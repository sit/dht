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

#ifndef __CHORD_H
#define __CHORD_H

#include "p2psim/p2protocol.h"
#include "consistenthash.h"

//#define CHORD_DEBUG
#define PKT_OVERHEAD 20

#define TYPE_USER_LOOKUP 0
#define TYPE_JOIN_LOOKUP 1
#define TYPE_FINGER_LOOKUP 2
#define TYPE_FIXSUCC_UP 3
#define TYPE_FIXSUCCLIST_UP 4
#define TYPE_FIXPRED_UP 5
#define TYPE_FINGER_UP 6
#define TYPE_PNS_UP 7

#define MIN_BASIC_TIMER 100

class LocTable;

class Chord : public P2Protocol {
public:
  
  typedef ConsistentHash::CHID CHID;

  class IDMap{
    public:
    ConsistentHash::CHID id; //consistent hashing ID for the node
    IPAddress ip; //the IP address for the node
    uint choices; //this is a gross hack to help me keep track of how many choices this finger has
    static bool cmp(const IDMap& a, const IDMap& b) { return (a.id <= b.id);}
    bool operator==(const IDMap a) { return (a.id == id); }
  };

  Chord(IPAddress i, Args& a, LocTable *l = NULL); 
  virtual ~Chord();
  virtual string proto_name() { return "Chord"; }

  // Functions callable from events file.
  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void lookup(Args*);
  virtual void insert(Args*) {};


  struct get_predecessor_args {
    int dummy;
  };
  struct get_predecessor_ret {
    IDMap n;
  };

  struct get_successor_list_args {
    uint m; //number of successors wanted
  };
  
  struct get_successor_list_ret {
    vector<IDMap> v;
  };

  struct notify_args {
    IDMap me;
  };
  struct notify_ret {
    int dummy;
  };

  struct alert_args {
    IDMap n;
  };

  struct hop_info {
    IDMap from;
    IDMap to;
    uint hop;
  };

  
  struct next_args {
    CHID key;
    uint m;
    uint all; //get m out of the first all successors
    bool retry;
    uint type;
    vector<IDMap> deadnodes;
  };

  struct next_ret {
    bool done;
    vector<IDMap> v;
    vector<IDMap> next;
    bool correct;
    IDMap lastnode;
  };

  struct nextretinfo{
    hop_info link;
    next_ret ret;
  };

  struct find_successors_args {
    CHID key;
    uint m;
    uint all;
  };

  struct find_successors_ret {
    vector<IDMap> v;
    IDMap last;
  };

  struct lookup_path {
    IDMap n;
    bool tout;
  };

  struct next_recurs_args {
    IPAddress src;
    uint type;
    CHID key;
    vector<lookup_path> path;
    uint m;
    uint all;
  };

  struct next_recurs_ret {
    vector<IDMap> v;
    bool correct;
    uint finish_time;
  };

  struct lookup_args{
    CHID key;
    Time start;
    vector<uint> retrytimes;
  };
  // RPC handlers.
  void null_handler (void *args, void *ret);
  void get_predecessor_handler(get_predecessor_args *, 
                               get_predecessor_ret *);
  void get_successor_list_handler(get_successor_list_args *, 
                                  get_successor_list_ret *);
  void notify_handler(notify_args *, notify_ret *);
  void alert_handler(alert_args *, void *);
  void next_handler(next_args *, next_ret *);
  void find_successors_handler(find_successors_args *, 
                               find_successors_ret *);

  void final_recurs_hop(next_recurs_args *args, next_recurs_ret *ret);
  virtual void my_next_recurs_handler(next_recurs_args *, next_recurs_ret *);
  void next_recurs_handler(next_recurs_args *, next_recurs_ret *);
  void lookup_internal(lookup_args *a);
  void ping_delete(alert_args *aa);

  CHID id () { return me.id; }
  virtual void initstate();
  virtual bool stabilized(vector<CHID>);
  bool check_correctness(CHID k, vector<IDMap> v);

  virtual void dump();
  char *ts();

  void stabilize();
  virtual void reschedule_basic_stabilizer(void *);

  bool inited() {return _inited;};

protected:
  //chord parameters
  uint _nsucc;
  uint _allfrag;
  uint _timeout;
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
  uint _last_succlist_stabilized;

  LocTable *loctable;
  IDMap me; 
  CHID _prev_succ;
  uint i0;
  vector<IDMap> lastscs;
  bool _isstable;
  bool _inited;

  //global statistics of all Chord nodes
  static vector<double> stat;
  static double _lookup_num;
  static double _lookup_int_num;
  static double _lookup_hops;
  static double _lookup_timeouts;
  static double _lookup_to_waste;
  static double _lookup_interval;
  static vector<double> _lookup_lat_v;
  static double _lookup_success;
  static double _lookup_raw_success;
  static double _lookup_raw_num;
  static double _lookup_retries;


  virtual vector<IDMap> find_successors_recurs(CHID key, uint m, uint all,
      uint type, uint *lookup_int = NULL);
  virtual vector<IDMap> find_successors(CHID key, uint m, uint all,
      uint type, uint *lookup_int = NULL, IDMap *last = NULL);

  void fix_successor(void *x=NULL);
  void fix_predecessor();
  void fix_successor_list();
  void check_static_init();
  void record_stat(uint bytes, uint type);
  void record_lookupstat(uint num, uint type);

private:
  Time _last_join_time;
};

typedef struct {
  ConsistentHash::CHID id;
  uint pin_succ;
  uint pin_pred;
} pin_entry;

class LocTable {

  public:

    struct idmapwrap {
      Chord::IDMap n;
      ConsistentHash::CHID id;
      Time timestamp;
      sklist_entry<idmapwrap> sortlink_;
      bool is_succ;
      bool pinned;
      idmapwrap(Chord::IDMap x, Time t = 0) {
	n.ip = x.ip;
	n.id = x.id;
	n.choices = x.choices;
	id = x.id;
	pinned = false;
	timestamp = t;
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

    Chord::IDMap succ(ConsistentHash::CHID id, Time *ts = NULL);
    vector<Chord::IDMap> succs(ConsistentHash::CHID id, unsigned int m, Time *ts = NULL);
    vector<Chord::IDMap> preds(Chord::CHID id, uint m);
    Chord::IDMap pred(Chord::CHID id);
    void checkpoint();
    void print();

    void add_node(Chord::IDMap n, bool is_succ=false);
    void add_sortednodes(vector<Chord::IDMap> l);
    void del_node(Chord::IDMap n);
    virtual void del_all();
    void notify(Chord::IDMap n);
    void pin(Chord::CHID x, uint pin_succ, uint pin_pred);
    void clear_pins();
    uint size();
    uint psize() { return pinlist.size();}
    void set_evict(bool v) { _evict = v; }
    void set_timeout(uint to) {_timeout = to;}

    //pick the next hop for lookup;
    virtual vector<Chord::IDMap> next_hops(Chord::CHID key, uint nsz = 1);
    virtual Chord::IDMap next_hop(Chord::CHID key, uint m = 1, uint nsucc=1); 

    vector<Chord::IDMap> get_all();
    Chord::IDMap first();
    Chord::IDMap last();
    Chord::IDMap search(ConsistentHash::CHID);
    void dump();
    void stat();

  protected:
    bool _evict;
    skiplist<idmapwrap, ConsistentHash::CHID, &idmapwrap::id, &idmapwrap::sortlink_, idmapcompare> ring;
    Chord::IDMap me;
    vector<pin_entry> pinlist;
    uint _max;
    uint _timeout;

    //evict one node to make sure ring contains <= _max elements
    void evict(); 
};

#endif // __CHORD_H

