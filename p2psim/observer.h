#ifndef __OBSERVER_H
#define __OBSERVER_H

#include "p2psim.h"

class Observer {
public:
  Observer();
  virtual ~Observer();

  void reschedule(Time);
  virtual void execute() = 0;
};

#endif // __OBSERVER_H
