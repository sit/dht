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

  Time ts;
  unsigned id() { return _id; }
  static void Execute(Event *e);

 protected:
  virtual ~Event();

 private:
  unsigned _id;
  static unsigned _uniqueid;

  // Rule: when execute() finishes, the Event must be
  // finished. This is to simplify the decision of when to free.
  // Also you're guaranteed that execute() runs in its own thread.
  virtual void execute() = 0;

  static void Execute1(Event *e);
};

#endif // __EVENT_H
