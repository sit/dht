#include "threadmanager.h"
#include <assert.h>
using namespace std;

Threaded::Threaded()
{
  _exitchan = chancreate(sizeof(unsigned), 0);
  assert(_exitchan);
}

Threaded::~Threaded()
{
  chanfree(_exitchan);
  // if this assert is pestering you, you did something very wrong.
  // never remove this assert.
  assert(threadid() == _thread);
}


void
Threaded::thread()
{
  _thread = ThreadManager::Instance()->create(Threaded::Run, this);
}


void
Threaded::Run(void *t)
{
  ((Threaded *) t)->run();
}
