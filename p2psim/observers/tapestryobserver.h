#ifndef __TAPESTRY_OBSERVER_H
#define __TAPESTRY_OBSERVER_H

#include "../p2psim/oldobserver.h"
#include "../protocols/tapestry.h"

class TapestryObserver : public Oldobserver {
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

  void init_state();

  vector<Tapestry::GUID> lid;
};

#endif // __TAPESTRY_OBSERVER_H
