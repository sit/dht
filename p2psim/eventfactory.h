#ifndef __EVENT_FACTORY_H
#define __EVENT_FACTORY_H

#include <string>
#include <vector>
#include "event.h"

using namespace std;

class EventFactory {
public:
  static EventFactory* Instance();
  static void DeleteInstance();
  Event *create(string, vector<string>*);

private:
  static EventFactory *_instance;
  EventFactory();
  ~EventFactory();
};

#endif // __EVENT_FACTORY_H
