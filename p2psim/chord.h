#ifndef __CHORD_H
#define __CHORD_H

#include "protocol.h"
#include "consistenthash.h"

#include <vector>

#define CHORD_SUCC_NUM 3  // default number of successors maintained
#define STABLE_TIMER 500
#define PAD "000000000000000000000000"

#include "p2psim.h"

class LocTable;

class Chord : public Protocol {
public:
  typedef ConsistentHash::CHID CHID;
  struct IDMap {
    ConsistentHash::CHID id; //consistent hashing ID for the node
    IPAddress ip; //the IP address for the node
  };

  Chord(Node *n, uint numsucc = CHORD_SUCC_NUM);
  virtual ~Chord();

  // Functions callable from events file.
  virtual void join(Args*);
  virtual void leave(Args*) {};
  virtual void crash(Args*) {};
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
  struct next_args {
    CHID key;
    int m;
    IDMap who;
  };
  struct next_ret {
    bool done;
    vector<IDMap> v;
    IDMap next;
  };

  // RPC handlers.
  void get_predecessor_handler(get_predecessor_args *, get_predecessor_ret *);
  void get_successor_list_handler(get_successor_list_args *, get_successor_list_ret *);
  //void get_successor_list_handler(get_predecessor_args *, get_predecessor_ret *);
  void notify_handler(notify_args *, notify_ret *);
  void next_handler(next_args *, next_ret *);

  CHID id () { return me.id; }
  virtual bool stabilized(vector<CHID>);

  virtual void dump();
  char *ts();

  virtual void stabilize();
  virtual void reschedule_stabilizer(void *);

protected:
  LocTable *loctable;
  IDMap me;
  uint nsucc;

  virtual vector<IDMap> find_successors(CHID key, uint m, bool intern);
  void fix_successor();
  void fix_successor_list();
};

typedef struct {
  ConsistentHash::CHID id;
  bool pin_succ; //false for pinning down predecessors, true for pinning down successors
  unsigned int pin_num;
}pin_entry;

class LocTable {

  public:

    LocTable(Chord::IDMap me);
    ~LocTable() {};

    Chord::IDMap succ(ConsistentHash::CHID id);
    vector<Chord::IDMap> succs(ConsistentHash::CHID id, unsigned int m);
    Chord::IDMap pred();
    Chord::IDMap pred(Chord::CHID n);
    void checkpoint();
    void print();

    void add_node(Chord::IDMap n);
    void del_node(Chord::IDMap n);
    void notify(Chord::IDMap n);
    void pin(Chord::CHID x, bool pin_succ = true, unsigned int pin_num = 1);
/*
    struct idmapwrap {
      sklist_entry<idmapwrap> sortlink_;
      Chord::CHID id_;
      IPAddress ip_;
    };
    struct chidwrap {
      sklist_entry<chidwrap> sortlink_;
      Chord::CHID id_;
    };
    skiplist<idmapwrap, Chord::CHID, &idmapwrap::id_, &idmapwrap::sortlink_> loclist;  emil's skiplist needs sfs's keyfunc.h
    skiplist<chidwrap, Chord::CHID, &chidwrap::id_, &chidwrap::sortlink_> pinlist;
    */
  private:
    vector<Chord::IDMap> ring;
    vector<pin_entry> pinlist;
    unsigned int _max;

    void evict(); //evict one node to make sure ring contains <= _max elements
};

#endif // __CHORD_H





