#ifndef __CHORD_H
#define __CHORD_H

#include "p2psim.h"
#include "protocol.h"

typedef unsigned HashedID;

typedef struct{
  HashedID hid; //hashed ID
  NodeID id; //node ID, like an IP address
}IDMap;

class Chord : public Protocol {
public:
  Chord(Node *n) : Protocol(n) {};
  ~Chord();

  virtual void join() {};
  virtual void leave() {};
  virtual void crash() {};
  virtual void lookup() {};
  virtual void insert_doc() {};
  virtual void lookup_doc() {};
  virtual void stabilize() {};
  virtual Packet* receive(Packet*) {};

protected:
  IDMap me;
  IDMap successor;
  IDMap predecessor;
};

#endif // __CHORD_H
