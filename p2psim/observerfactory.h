#ifndef __OBSERVER_FACTORY_H
#define __OBSERVER_FACTORY_H

#include "observer.h"
#include "args.h"
using namespace std;

class ObserverFactory {
public:
  static Observer *create(string, Args*);
};

#endif // __OBSERVER_FACTORY_H
