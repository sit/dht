#ifndef __CBEVENT_H
#define __CBEVENT_H

#include "event.h"

class CBEvent : public Event {
public:
  CBEvent();
  ~CBEvent();

  virtual void execute();

  Protocol *prot;
  Protocol::member_f fn;
  void *args; // ???
};

#endif // __CBEVENT_H
