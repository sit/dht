#ifndef __OBSERVER_H
#define __OBSERVER_H

#include "p2psim.h"
#include "args.h"

#define OBSERVER_TYPE   "__type"

class Observer {
public:
  Observer(Args *a);
  virtual ~Observer();

  void reschedule(Time);
  virtual void execute() = 0;

protected:
  string _type;
};

#endif // __OBSERVER_H
