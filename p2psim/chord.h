#ifndef __CHORD_H
#define __CHORD_H

#include "p2psim.h"
#include "protocol.h"
#include "consistenthash.h"


class LocTable;

class Chord : public Protocol {
public:
  typedef ConsistentHash::CHID CHID;
  typedef struct {
    CHID id; //consistent hashing ID for the node
    IPAddress ip; //the IP address for the node
  } IDMap;

  Chord(Node *n);
  virtual ~Chord();

  virtual void join(Args*);
  virtual void leave(Args*) {};
  virtual void crash(Args*) {};
  virtual void lookup(Args*) {};
  virtual void insert_doc(Args*) {};
  virtual void lookup_doc(Args*) {};

  // RPC handlers.
  void *find_successor_x(void *);
  void *get_predecessor(void *);
  void *notify(void *);

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
