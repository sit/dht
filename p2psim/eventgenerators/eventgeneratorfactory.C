#include "eventgeneratorfactory.h"
#include "fileeventgenerator.h"
#include "churneventgenerator.h"

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
  Args *a = New Args(v);
  assert(a);

  if(type == "FileEventGenerator")
    eg = New FileEventGenerator(a);

  if(type == "ChurnEventGenerator")
    eg = New ChurnEventGenerator(a);

  delete a;
  return eg;
}
