#include "eventgeneratorfactory.h"
#include "fileeventgenerator.h"
#include "p2psim.h"

EventGeneratorFactory *EventGeneratorFactory::_instance = 0;

EventGeneratorFactory*
EventGeneratorFactory::Instance()
{
  if(!_instance)
    _instance = New EventGeneratorFactory();
  return _instance;
}

EventGeneratorFactory::EventGeneratorFactory()
{
}

EventGeneratorFactory::~EventGeneratorFactory()
{
}


EventGenerator *
EventGeneratorFactory::create(string type, vector<string> *v)
{
  EventGenerator *eg = 0;
  if(type == "file")
    eg = New FileEventGenerator(v);

  return eg;
}
