#ifndef __P2PEVENT_H
#define __P2PEVENT_H

#include <vector>
#include <string>
#include "event.h"
#include "protocol.h"

class P2PEvent : public Event {
public:
  P2PEvent();
  P2PEvent(vector<string>*);
  ~P2PEvent();

  virtual void execute();

  Node *node;
  string protocol;
  Protocol::EventID event;
  Protocol::Args *args;
};

#endif // __P2PEVENT_H
