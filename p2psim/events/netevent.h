#ifndef __NETEVENT_H
#define __NETEVENT_H

#include "p2psim/event.h"

class NetEvent : public Event {
public:
  NetEvent();

  Node *node;
  Packet *p;

 protected:
  ~NetEvent();

 private:
  virtual void execute();
};

#endif // __NETEVENT_H
