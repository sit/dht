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
  Chord(Node *n) : Protocol(n) {};
  ~Chord();

  virtual void join(void*) {};
  virtual void leave(void*) {};
  virtual void crash(void*) {};
  virtual void lookup(void*) {};
  virtual void insert_doc(void*) {};
  virtual void lookup_doc(void*) {};

protected:
  IDMap me;
  IDMap successor;
  IDMap predecessor;
};

#endif // __CHORD_H
