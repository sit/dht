#ifndef __ANJALIONEHOP_H
#define __ANJALIONEHOP_H


#include "p2psim/p2protocol.h"
#include "consistenthash.h"
#include "chord.h"

class AnjaliLocTable : LocTable {
  public:
  AnjaliLocTable(uint k, uint u) : LocTable() {
    _k = k; //number of slices
    _u = u; //number of units
  };
  ~AnjaliLocTable() {};
  vector<IDMap> sliceleaders() {
  };
  vector<IDMap> unitleaders() {
  };
  bool is_sliceleader() {
    for (uint i = 0; i < _k; i++) {
    }
  };
}

class AnjaliOneHop : public P2Protocol {
public:
  typedef Chord::IDMap IDMap;
  AnjaliOneHop(Node *n, Args& a);
  ~AnjaliOneHop();
  string proto_name() { return "AnjaliOneHop";}

  // Functions callable from events file.
  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void lookup(Args*);
  virtual void insert(Args*) {};

  struct notifyevent_args {
    vector<deadalive_event> v;
  };

  void reschedule_stabilizer(void *x);
  void stabilize();
  void notifyleaders(vector<IDMap> leaders, vector<deadalive_event> es);

  //RCP handlers
  void ping_handler(notifyevent_args *args, void *ret);
  void notifyevent_handler(notifyevent_args *args, void *ret);
  void notifyfromslice_handler(notifyevent_args *args, void *ret);

protected:
  AnjaliLocTable *loctable;
  IDMap me;
}


#endif /* __ANJALIONEHOP_H */


