#ifndef __OBSERVER_FACTORY_H
#define __OBSERVER_FACTORY_H

#include "oldobserver.h"
#include "args.h"
#include <set>
using namespace std;

class OldobserverFactory {
public:
  static OldobserverFactory* Instance();
  Oldobserver *create(string, Args*);
  ~OldobserverFactory();

private:
  OldobserverFactory();
  static OldobserverFactory *_instance;
  set<Oldobserver*> _oldobservers;
};

#endif // __OBSERVER_FACTORY_H
