#include "threadmanager.h"
#include <assert.h>
using namespace std;

Threaded::Threaded()
{
}

Threaded::~Threaded()
{
  // if this assert is pestering you, you did something very wrong.
  // never remove this assert.
  // Thomer: what did I do wrong? why shouldn't I remove this assert? RTM
  //  assert(threadid() == _thread);
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
