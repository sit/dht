#ifndef __OBSERVER_H
#define __OBSERVER_H

#include "threaded.h"
#include "p2psim.h"

class Observer {
public:
  Observer();
  ~Observer();

  void reschedule(Time);
  virtual void execute() = 0;
};

#endif // __OBSERVER_H
