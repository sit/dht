#ifndef __P2PEVENT_H
#define __P2PEVENT_H

#include <vector>
#include <string>
#include "args.h"
#include "event.h"
#include "protocol.h"

class P2PEvent : public Event {
public:
  P2PEvent();
  P2PEvent(vector<string>*);

  Node *node;
  string protocol;
  Protocol::event_f fn;
  Args *args;

 protected:
  ~P2PEvent();

 private:
  virtual void execute();
  Protocol::event_f name2fn(string name);
};

#endif // __P2PEVENT_H
