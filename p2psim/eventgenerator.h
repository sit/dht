#ifndef __EVENT_GENERATOR_H
#define __EVENT_GENERATOR_H

#include "observer.h"
#include <fstream>
#include <string>
using namespace std;

class EventGenerator : public Observer {
public:
  EventGenerator() {};
  virtual ~EventGenerator() {};

  // to parse the first line and generate an EventGenerator subtype
  static EventGenerator* EventGenerator::parse(char *filename);

  // to the rest of the file
  virtual void parse(ifstream&) = 0;
};

#endif //  __EVENT_GENERATOR_H
