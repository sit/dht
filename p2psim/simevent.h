#ifndef __SIMEVENT_H
#define __SIMEVENT_H

#include "event.h"
#include <vector>
#include <string>
using namespace std;

class SimEvent : public Event {
public:
  SimEvent();
  SimEvent(vector<string>*);
  ~SimEvent();

  virtual void execute();

private:
  string _op;
};

#endif // __SIMEVENT_H
