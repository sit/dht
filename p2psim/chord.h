#ifndef __CHORD_H
#define __CHORD_H

#include "dhtprotocol.h"
#include "consistenthash.h"
#include "../utils/skiplist.h"

#include <vector>

#define CHORD_SUCC_NUM 3  // default number of successors maintained
#define STABLE_TIMER 500
#define CHORD_DEBUG

#include "p2psim.h"

class LocTable;

class Chord : public DHTProtocol {
public:
  typedef ConsistentHash::CHID CHID;

  class IDMap{
    public:
    ConsistentHash::CHID id; //consistent hashing ID for the node
    IPAddress ip; //the IP address for the node
    static bool cmp(const IDMap& a, const IDMap& b) { return (a.id <= b.id);}
    bool operator==(const IDMap a) { return (a.id == id); }
  };

  Chord(Node *n, uint numsucc = CHORD_SUCC_NUM);
  virtual ~Chord();
  string proto_name() { return "Chord"; }

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

  // RPC handlers.
  void get_predecessor_handler(get_predecessor_args *, get_predecessor_ret *);
  void get_successor_list_handler(get_successor_list_args *, get_successor_list_ret *);
  //void get_successor_list_handler(get_predecessor_args *, get_predecessor_ret *);
  void notify_handler(notify_args *, notify_ret *);
  void alert_handler(alert_args *, alert_ret *);
  void next_handler(next_args *, next_ret *);
  void find_successors_handler(find_successors_args *, find_successors_ret *);

  CHID id () { return me.id; }
  virtual void init_state(vector<IDMap> ids);
  virtual bool stabilized(vector<CHID>);

  virtual void dump();
  char *ts();

  virtual void stabilize();
  virtual void reschedule_stabilizer(void *);

protected:
  LocTable *loctable;
  IDMap me;
  uint nsucc;
  CHID _prev_succ;
  uint i0;
  vector<IDMap> lastscs;
  bool _isstable;

  virtual vector<IDMap> find_successors(CHID key, uint m, bool intern);
  void fix_successor();
  void fix_successor_list();
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
      sklist_entry<idmapwrap> sortlink_;
      bool pinned;
      idmapwrap(Chord::IDMap x) {
	n.ip = x.ip;
	n.id = x.id;
	id = x.id;
	pinned = false;
      }
    };

    struct idmapcompare{
      idmapcompare() {}
      int operator() (ConsistentHash::CHID a, ConsistentHash::CHID b) const
      { return a < b ? -1 : b < a; }
    };


    LocTable(Chord::IDMap me);
    ~LocTable();

    Chord::IDMap succ(ConsistentHash::CHID id);
    vector<Chord::IDMap> succs(ConsistentHash::CHID id, unsigned int m);
    Chord::IDMap pred();
    Chord::IDMap pred(Chord::CHID n);
    void checkpoint();
    void print();

    void add_node(Chord::IDMap n);
    void add_sortednodes(vector<Chord::IDMap> l);
    void del_node(Chord::IDMap n);
    void notify(Chord::IDMap n);
    void pin(Chord::CHID x, uint pin_succ, uint pin_pred);
    void clear_pins();
    uint size();
    unsigned int psize() { return pinlist.size();}

  private:
    skiplist<idmapwrap, ConsistentHash::CHID, &idmapwrap::id, &idmapwrap::sortlink_> ring;
    ConsistentHash::CHID myid;
    vector<pin_entry> pinlist;
    unsigned int _max;
    unsigned int rsz;

    void evict(); //evict one node to make sure ring contains <= _max elements
};

#endif // __CHORD_H

