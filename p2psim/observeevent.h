#ifndef __OBSERVEEVENT_H
#define __OBSERVEEVENT_H


#include "event.h"
#include "observer.h"
#include <vector>
#include <string>
using namespace std;

class ObserveEvent : public Event {
public:
  ObserveEvent();
  ObserveEvent(vector<string>*);
  ObserveEvent(Observer*);
  ~ObserveEvent();

  virtual void execute();

private:
  Observer *_observer;
};

#endif // __OBSERVEEVENT_H
