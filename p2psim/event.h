#ifndef __EVENT_H
#define __EVENT_H

#include "node.h"
using namespace std;

class Event {
public:
  friend class EventQueue;

  Event();
  Event(vector<string>*);

  Time ts;  // absolute time
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
  static void Execute1(void *e);
};

#endif // __EVENT_H
