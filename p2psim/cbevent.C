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
  // XXX: THIS IS FUCKING SCARY AND PROBABLY WRONG
  threadcreate(CBEvent::Dispatch, (void*)this, mainstacksize);
}


void
CBEvent::Dispatch(void *ex)
{
  CBEvent *e = (CBEvent*) ex;
  (e->prot->*e->fn)(e->args);
  threadexits(0);
}
