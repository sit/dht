#ifndef __EVENT_GENERATOR_H
#define __EVENT_GENERATOR_H

#include "observer.h"
#include <fstream>
#include <string>
#include "threaded.h"
#include "eventqueueobserver.h"
using namespace std;

class EventGenerator : public EventQueueObserver, public Threaded {
public:
  EventGenerator() {};
  virtual ~EventGenerator() {};

  // to parse the first line and generate an EventGenerator subtype
  static EventGenerator* EventGenerator::parse(char *filename);
};

#endif //  __EVENT_GENERATOR_H
