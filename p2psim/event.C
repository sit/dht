#include "event.h"

unsigned Event::_uniqueid = 0;

Event::Event()
{
  _id = _uniqueid++;
}

Event::~Event()
{
}
