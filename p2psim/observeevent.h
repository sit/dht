#ifndef __OBSERVEEVENT_H
#define __OBSERVEEVENT_H


#include "event.h"
#include "observer.h"
using namespace std;

class ObserveEvent : public Event {
public:
  ObserveEvent();
  ObserveEvent(string, vector<string>*);
  ObserveEvent(Observer*);
  ~ObserveEvent();

  virtual void execute();

private:
  Observer *_observer;
};

#endif // __OBSERVEEVENT_H
