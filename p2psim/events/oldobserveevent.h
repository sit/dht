#ifndef __OBSERVEEVENT_H
#define __OBSERVEEVENT_H

#include "p2psim/event.h"
#include "p2psim/oldobserver.h"
using namespace std;

class OldobserveEvent : public Event {
public:
  OldobserveEvent();
  OldobserveEvent(string, vector<string>*);
  OldobserveEvent(Oldobserver*);
  ~OldobserveEvent();

  virtual void execute();

private:
  Oldobserver *_oldobserver;
};

#endif // __OBSERVEEVENT_H
