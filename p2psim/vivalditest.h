#ifndef __VIVALDITEST_H
#define __VIVALDITEST_H

#include "protocol.h"
#include "node.h"

class VivaldiTest : public Protocol {
public:
  VivaldiTest(Node*);
  ~VivaldiTest();

  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void insert(Args*);
  virtual void lookup(Args*);
};

#endif // __VIVALDITEST_H
