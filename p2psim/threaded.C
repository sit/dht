#include "threaded.h"
#include "threadmanager.h"
#include <assert.h>

Threaded::Threaded()
{
  _exitchan = chancreate(sizeof(unsigned), 0);
  assert(_exitchan);
}

Threaded::~Threaded()
{
  chanfree(_exitchan);
  threadexits(0);
}

void
Threaded::thread()
{
  _thread = ThreadManager::Instance()->create(this, Threaded::Run, this);
}

void
Threaded::Run(void *t)
{
  ((Threaded *) t)->run();
}

