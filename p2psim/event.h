#ifndef __EVENT_H
#define __EVENT_H

#include "node.h"
#include "p2psim.h"
#include <vector>
#include <string>
using namespace std;

class Event {
public:
  Event();
  Event(vector<string>*);
  virtual ~Event();

  Time ts;
  unsigned id() { return _id; }
  virtual void execute() = 0;

private:
  unsigned _id;
  static unsigned _uniqueid;
};

#endif // __EVENT_H
