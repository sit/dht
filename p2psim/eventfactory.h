#ifndef __EVENT_FACTORY_H
#define __EVENT_FACTORY_H

#include "event.h"
class EventFactory {
public:
  static EventFactory* Instance();
  static void DeleteInstance();
  Event *create(string type, vector<string>* v, string proto="");

private:
  static EventFactory *_instance;
  EventFactory();
  ~EventFactory();
};

#endif // __EVENT_FACTORY_H
