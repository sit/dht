#ifndef __CHORD_H
#define __CHORD_H

#include "p2psim.h"
#include "protocol.h"
#include "consistenthash.h"


class LocTable;

class Chord : public Protocol {
public:
  typedef ConsistentHash::CHID CHID;
  struct IDMap {
    CHID id; //consistent hashing ID for the node
    IPAddress ip; //the IP address for the node
  };

  Chord(Node *n);
  virtual ~Chord();

  virtual void join(Args*);
  virtual void leave(Args*) {};
  virtual void crash(Args*) {};
  virtual void lookup(Args*) {};
  virtual void insert_doc(Args*) {};
  virtual void lookup_doc(Args*) {};

  // Chord RPC argument/return types.
  struct find_successor_args {
    Chord::CHID n;
  };
  struct find_successor_ret {
    Chord::IDMap succ;
  };
  struct get_predecessor_args {
    int dummy;
  };
  struct get_predecessor_ret {
    Chord::IDMap n;
  };
  struct lookup_args {
    Chord::CHID key;
  };
  struct lookup_ret {
    Chord::IDMap succ;
  };
  struct notify_args {
    Chord::IDMap me;
  };
  struct notify_ret {
    int dummy;
  };

  // RPC handlers.
  void find_successor_handler(find_successor_args *, find_successor_ret *);
  void get_predecessor_handler(get_predecessor_args *, get_predecessor_ret *);
  void lookup_handler(lookup_args *, lookup_ret *);
  void notify_handler(notify_args *, notify_ret *);

  string s();

protected:
  LocTable *loctable;
  IDMap me;

  IDMap next(CHID n);
  void fix_predecessor();
  void fix_successor();
  void stabilize();
};


class LocTable {

  public:

    LocTable(Chord::IDMap me) {
      _max = 3; 
      _succ_num = 1;

      // XXX: shouldn't the just be a new?
      ring = (Chord::IDMap *)malloc(sizeof(Chord::IDMap) * _max);
      assert(ring);
      bzero(ring, sizeof(Chord::IDMap) * _max); //init 
      ring[0] = me; //ring[0] is always be me, ring[1] is always succ
      _end = 2; 

    }; 

    ~LocTable() {
      free(ring);
    }

    void resize(unsigned int max, unsigned int s, unsigned int f) {
      _max = max;
      _succ_num = s;
      _finger_num = f;
      ring = (Chord::IDMap *)realloc(ring, sizeof(Chord::IDMap) * _max); //this is not a general resize, it has to be called immediately after construction
      assert(ring);
    };

    Chord::IDMap succ(unsigned int m);
    Chord::IDMap pred();
    Chord::IDMap next(Chord::CHID n);

    void add_node(Chord::IDMap n);
    void del_node(Chord::IDMap n);
    void notify(Chord::IDMap n);

  private:
    // XXX: why not a vector and void all this?
    Chord::IDMap *ring; //forgive me for not using STL vector 
    unsigned int _succ_num;
    unsigned int _finger_num;
    unsigned int _max;
    unsigned int _end; //if the ring array is full, _predecessor should always equal to _max-1;
};

#endif // __CHORD_H
