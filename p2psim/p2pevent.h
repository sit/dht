#ifndef __P2PEVENT_H
#define __P2PEVENT_H

#include "event.h"

class P2PEvent : public Event {
public:
  P2PEvent();
  ~P2PEvent();

  virtual void execute();

  string protocol;
  Protocol::EventID event;
  void *args; // ???
};

#endif // __P2PEVENT_H
