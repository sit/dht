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
  virtual void insert_doc(Args*);
  virtual void lookup_doc(Args*);

  void join_kademlia(void *);
  void do_join(void*, void*);
};

#endif // __KADEMLIA_H
