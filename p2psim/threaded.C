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
  thread(this);
}


void
Threaded::thread(Threaded *t)
{
  _thread = ThreadManager::Instance()->create(t, Threaded::Run, this);
}


void
Threaded::Run(void *t)
{
  ((Threaded *) t)->run();
}

