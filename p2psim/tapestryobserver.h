#ifndef __TAPESTRY_OBSERVER_H
#define __TAPESTRY_OBSERVER_H

#include "observer.h"

class TapestryObserver : public Observer {
public:
  static TapestryObserver* Instance(Args*);
  virtual void execute();

private:
  static TapestryObserver *_instance;
  TapestryObserver(Args*);
  ~TapestryObserver();
  unsigned int _reschedule;
  unsigned int _num_nodes;
  unsigned int _init_num;

  vector<Tapestry::GUID> lid;
};

#endif // __TAPESTRY_OBSERVER_H
