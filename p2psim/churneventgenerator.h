#ifndef __CHURN_EVENT_GENERATOR_H
#define __CHURN_EVENT_GENERATOR_H

#include "eventgenerator.h"
#include "p2psim.h"
#include "args.h"
using namespace std;

class ChurnEventGenerator : public EventGenerator {
public:
  ChurnEventGenerator(Args *);
  virtual void kick(Observed *, ObserverInfo*);
  virtual void run();

private:
  unsigned _size;
  string _proto;
};

#endif // __CHURN_EVENT_GENERATOR_H
