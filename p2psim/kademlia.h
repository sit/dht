#ifndef __KADEMLIA_H
#define __KADEMLIA_H

#include "protocol.h"
#include "node.h"

class Kademlia : public Protocol {
public:
  Kademlia(Node*);
  ~Kademlia();

  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void insert(Args*);
  virtual void lookup(Args*);

  void join_kademlia(void *);
  void do_join(IPAddress *, void*);
};

#endif // __KADEMLIA_H
