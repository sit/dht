#ifndef __VIVALDITEST_H
#define __VIVALDITEST_H

#include "protocol.h"
#include "node.h"
#include "vivaldi.h"

class VivaldiTest : public Protocol {
public:
  VivaldiTest(Node*);
  ~VivaldiTest();

  virtual void join(Args*) { }
  virtual void leave(Args*) { }
  virtual void crash(Args*) { }
  virtual void insert(Args*) { }
  virtual void lookup(Args*) { }

 private:
  Vivaldi *_vivaldi;

  void tick(void *);
  char * ts();
};

#endif // __VIVALDITEST_H
