#include "threaded.h"

Threaded::Threaded()
{
}

Threaded::~Threaded()
{
}

void
Threaded::thread()
{
  _thread = threadcreate(Threaded::Run, this, mainstacksize);
}

void
Threaded::Run(void *t)
{
  ((Threaded *) t)->run();
}
