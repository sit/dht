#ifndef __EVENT_FACTORY_H
#define __EVENT_FACTORY_H

#include "p2psim/event.h"
class EventFactory {
public:
  static EventFactory* Instance();
  Event *create(string type, vector<string>* v, string proto="");
  ~EventFactory();

private:
  static EventFactory *_instance;
  EventFactory();
};

#endif // __EVENT_FACTORY_H
