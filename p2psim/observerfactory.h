#ifndef __OBSERVER_FACTORY_H
#define __OBSERVER_FACTORY_H

#include "observer.h"
#include "args.h"
#include <set>
using namespace std;

class ObserverFactory {
public:
  static ObserverFactory* Instance();
  Observer *create(string, Args*);
  ~ObserverFactory();

private:
  ObserverFactory();
  static ObserverFactory *_instance;
  set<Observer*> _observers;
};

#endif // __OBSERVER_FACTORY_H
