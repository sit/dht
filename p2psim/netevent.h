#ifndef __NETEVENT_H
#define __NETEVENT_H

#include "event.h"
#include "packet.h"

class NetEvent : public Event {
public:
  NetEvent();
  ~NetEvent();

  Packet *p;
  virtual void execute();
};

#endif // __NETEVENT_H
