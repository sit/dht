#ifndef __FILE_EVENT_GENERATOR_H
#define __FILE_EVENT_GENERATOR_H

#include "eventgenerator.h"
#include "p2psim.h"
#include <vector>
#include <string>
#include <fstream>
using namespace std;

class FileEventGenerator : public EventGenerator {
public:
  FileEventGenerator(vector<string> *);

  // to read properties
  virtual void parse(ifstream &in);

  // because we're an Observer
  virtual void kick(Observed *);
};


#endif // __FILE_EVENT_GENERATOR_H
