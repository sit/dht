#ifndef __FILE_EVENT_GENERATOR_H
#define __FILE_EVENT_GENERATOR_H

#include "eventgenerator.h"
#include "p2psim.h"
#include "args.h"
#include <string>
using namespace std;

class FileEventGenerator : public EventGenerator {
public:
  FileEventGenerator(Args*);
  virtual void kick(Observed *, ObserverInfo*);
  virtual void run();

private:
  string _name;
};


#endif // __FILE_EVENT_GENERATOR_H
