#ifndef __OLDOBSERVER_H
#define __OLDOBSERVER_H

#include "p2psim.h"
#include "args.h"

#define OBSERVER_TYPE   "__type"

class Oldobserver {
public:
  Oldobserver(Args *a);
  virtual ~Oldobserver();

  void reschedule(Time);
  virtual void execute() = 0;

protected:
  string _type;
};

#endif // __OLDOBSERVER_H
