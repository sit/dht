#ifndef __CHORD_H
#define __CHORD_H

#include "p2psim.h"
#include "protocol.h"

typedef unsigned HashedID;

typedef struct{
  HashedID hid; //hashed ID
  IPAddress id; //node ID, like an IP address
}IDMap;

class Chord : public Protocol {
public:
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

  string s();

protected:
  IDMap me;
  IDMap successor;
  IDMap predecessor;
};

#endif // __CHORD_H
