#include "cbevent.h"
#include <lib9.h>
#include <thread.h>
#include <iostream>
using namespace std;

CBEvent::CBEvent()
{
}

CBEvent::~CBEvent()
{
}

void
CBEvent::execute()
{
  // we can't do anything that blocks at this point, because we're running in
  // context of EventQueue.  we have to spin off a thread.
  threadcreate(CBEvent::Dispatch, (void*)this, mainstacksize);
}


void
CBEvent::Dispatch(void *ex)
{
  CBEvent *e = (CBEvent*) ex;
  (e->prot->*e->fn)(e->args, (void *) 0);
  threadexits(0);
}
