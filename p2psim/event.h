#ifndef __EVENT_H
#define __EVENT_H

#include "node.h"
#include "p2psim.h"

class Event {
public:
  Event();
  ~Event();

  Time ts;
  unsigned id() { return _id; }
  virtual void execute() = 0;

private:
  unsigned _id;
  static unsigned _uniqueid;
};

#endif // __EVENT_H
