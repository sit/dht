#ifndef __KADEMLIA_H
#define __KADEMLIA_H

#include "protocol.h"
#include "node.h"

class Kademlia : public Protocol {
public:
  Kademlia(Node*);
  ~Kademlia();

  virtual void join(void*);
  virtual void leave(void*);
  virtual void crash(void*);
  virtual void insert_doc(void*);
  virtual void lookup_doc(void*);

  void delayedcb(void*);
  void *do_join(void*);
};

#endif // __KADEMLIA_H
