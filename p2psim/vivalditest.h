#ifndef __VIVALDITEST_H
#define __VIVALDITEST_H

#include "protocol.h"
#include "node.h"
#include "vivaldi.h"
#include <vector>

class VivaldiTest : public Protocol {
public:
  VivaldiTest(Node*);
  ~VivaldiTest();

  virtual void join(Args*);
  virtual void leave(Args*) { }
  virtual void crash(Args*) { }
  virtual void insert(Args*) { }
  virtual void lookup(Args*) { }

  void status();
  Vivaldi::Coord real();
  double error();
  void total_error(double &x05, double &x50, double &x95);

 private:
  Vivaldi *_vivaldi;
  static vector<VivaldiTest*> _all;

  void tick(void *);
  char *ts();
  void handler(void *, Vivaldi::Coord *);
};

#endif // __VIVALDITEST_H
