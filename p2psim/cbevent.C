#include "cbevent.h"

CBEvent::CBEvent()
{
}

CBEvent::~CBEvent()
{
}

void
CBEvent::execute()
{
  (prot->*fn)(args);
}
