#include "event.h"
#include <vector>
#include <string>

using namespace std;

unsigned Event::_uniqueid = 0;

Event::Event()
{
  _id = _uniqueid++;
}

Event::Event(vector<string> *v)
{
  this->ts = (Time) atoi((*v)[0].c_str());
  v->erase(v->begin());
}

Event::~Event()
{
}
