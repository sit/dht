#ifndef __PASTRY_H
#define __PASTRY_H

#include "protocol.h"
#include "node.h"

class Pastry : public Protocol {
public:
  typedef long long NodeID;

  Pastry(Node*);
  ~Pastry();

  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void insert_doc(Args*);
  virtual void lookup_doc(Args*);
};

#endif // __PASTRY_H
