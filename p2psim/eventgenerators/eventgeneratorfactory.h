#ifndef __EVENT_GENERATOR_FACTORY_H
#define __EVENT_GENERATOR_FACTORY_H

#include "p2psim/eventgenerator.h"
#include <string>
#include <vector>
using namespace std;

class EventGeneratorFactory {
public:
  static EventGeneratorFactory* Instance();
  EventGenerator *create(string type, vector<string>* v);
  ~EventGeneratorFactory();

private:
  static EventGeneratorFactory *_instance;
  EventGeneratorFactory();
};

#endif //  __EVENT_GENERATOR_FACTORY_H
