#ifndef __CHORD_H
#define __CHORD_H

#include "vivaldi.h"
#include "dhtprotocol.h"
#include "consistenthash.h"

#define CHORD_SUCC_NUM 3  // default number of successors maintained
#define PAD "000000000000000000000000"

class LocTable;

class Chord : public DHTProtocol {
public:
  Vivaldi *_vivaldi;
  
  typedef ConsistentHash::CHID CHID;

  class IDMap{
    public:
    ConsistentHash::CHID id; //consistent hashing ID for the node
    IPAddress ip; //the IP address for the node
    uint choices; //this is a gross hack to help me keep track of how many choices this finger has
    static bool cmp(const IDMap& a, const IDMap& b) { return (a.id <= b.id);}
    bool operator==(const IDMap a) { return (a.id == id); }
  };

  Chord(Node *n, Args& a, LocTable *l = NULL); 
  virtual ~Chord();
  string proto_name() { return "Chord"; }

  // Functions callable from events file.
  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void lookup(Args*);
  virtual void insert(Args*) {};


  Vivaldi::Coord get_coords () {
    return _vivaldi->my_location ();
  }

  struct get_predecessor_args {
    int dummy;
  };
  struct get_predecessor_ret {
    IDMap n;
  };

  struct get_successor_list_args {
    unsigned int m; //number of successors wanted
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
  struct alert_ret {
    int dummy;
  };

  struct next_args {
    CHID key;
    int m;
  };
  struct next_ret {
    bool done;
    vector<IDMap> v;
    IDMap next;
  };

  struct find_successors_args {
    CHID key;
    int m;
  };

  struct find_successors_ret {
    vector<IDMap> v;
  };

  struct lookup_path {
    IDMap n;
    bool tout;
  };

  struct next_recurs_args {
    bool is_lookup;
    CHID key;
    vector<lookup_path> path;
    uint m;
  };

  struct next_recurs_ret {
    vector<IDMap> v;
    vector<lookup_path> path;
  };

  // RPC handlers.
  void null_handler (void *args, void *ret);
  void get_predecessor_handler(get_predecessor_args *, 
                               get_predecessor_ret *);
  void get_successor_list_handler(get_successor_list_args *, 
                                  get_successor_list_ret *);
  void notify_handler(notify_args *, notify_ret *);
  void alert_handler(alert_args *, alert_ret *);
  void next_handler(next_args *, next_ret *);
  void find_successors_handler(find_successors_args *, 
                               find_successors_ret *);
  void next_recurs_handler(next_recurs_args *, next_recurs_ret *);

  CHID id () { return me.id; }
  virtual void init_state(vector<IDMap> ids);
  virtual bool stabilized(vector<CHID>);

  virtual void dump();
  char *ts();

  virtual void stabilize();
  virtual void reschedule_stabilizer(void *);

protected:
  //chord parameters
  uint _nsucc;
  uint _allfrag;
  uint _vivaldi_dim;
  uint _timeout;
  uint _stabtimer;
  uint _frag;
  int _asap;
  bool _stab_running;

  LocTable *loctable;
  IDMap me; 
  CHID _prev_succ;
  uint i0;
  vector<IDMap> lastscs;
  bool _isstable;
  bool _inited;

  vector<uint> stat;

  virtual vector<IDMap> find_successors_recurs(CHID key, uint m, 
      bool is_lookup = false, uint *recurs_int = NULL);
  virtual vector<IDMap> find_successors(CHID key, uint m, 
      bool is_lookup = false);

  IDMap fix_successor();
  void fix_successor_list();
  void check_static_init();
  void record_stat(uint type = 0);
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
      { return a < b ? -1 : b < a; }
    };


  LocTable(uint timeout = 0);
  void init (Chord::IDMap me);
  virtual ~LocTable();

    Chord::IDMap succ(ConsistentHash::CHID id);
    vector<Chord::IDMap> succs(ConsistentHash::CHID id, unsigned int m);
    Chord::IDMap pred();
    Chord::IDMap pred(Chord::CHID n);
    void checkpoint();
    void print();

    void add_node(Chord::IDMap n);
    void add_sortednodes(vector<Chord::IDMap> l);
    void del_node(Chord::IDMap n);
    void del_all();
    void notify(Chord::IDMap n);
    void pin(Chord::CHID x, uint pin_succ, uint pin_pred);
    void clear_pins();
    uint size();
    uint psize() { return pinlist.size();}
    void set_evict(bool v) { _evict = v; }

    //pick the next hop for lookup;
    virtual Chord::IDMap next_hop(Chord::CHID key, bool *done, uint m = 1, uint nsucc=1); 

    vector<Chord::IDMap> get_all();

  protected:
    bool _evict;
    skiplist<idmapwrap, ConsistentHash::CHID, &idmapwrap::id, 
             &idmapwrap::sortlink_> ring;
    Chord::IDMap me;
    vector<pin_entry> pinlist;
    uint _max;
    uint _timeout;

    //evict one node to make sure ring contains <= _max elements
    void evict(); 
};

#endif // __CHORD_H

