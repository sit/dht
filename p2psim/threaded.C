#include "threaded.h"
#include <assert.h>

Threaded::Threaded()
{
  _exitchan = chancreate(sizeof(unsigned), 0);
  assert(_exitchan);
}

Threaded::~Threaded()
{
  chanfree(_exitchan);
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
