#ifndef __KADEMLIA_H
#define __KADEMLIA_H

#include "protocol.h"
#include "node.h"

class Kademlia : public Protocol {
public:
  Kademlia(Node*);
  ~Kademlia();

  virtual void join();
  virtual void leave();
  virtual void crash();
  virtual void insert_doc();
  virtual void lookup_doc();
  virtual void stabilize();
  virtual Packet* receive(Packet*);
};

#endif // __KADEMLIA_H
