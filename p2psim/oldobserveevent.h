#ifndef __OBSERVEEVENT_H
#define __OBSERVEEVENT_H


#include "event.h"
#include "oldobserver.h"
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
