/* $Id: condvar.C,v 1.2 2003/09/30 19:01:31 strib Exp $ */

#include "condvar.h"
using namespace std;

ConditionVar::ConditionVar()
{
  _waiters = New vector<Channel *>;
}

ConditionVar::~ConditionVar()
{
  // TODO: what happens if this gets deleted while there are threads waiting?
  // may have to do something smarter in the future
  assert( _waiters->size() == 0 );
  delete _waiters;
}

void
ConditionVar::wait()
{

  // create a channel, put it on the queue of waiting threads
  Channel *c = chancreate(sizeof(int*), 0);
  (*_waiters).push_back( c );
  recvp(c);
  chanfree(c);
}

void
ConditionVar::notifyAll()
{

  // wake all the sleeping guys
  for( uint i = 0; i < _waiters->size(); i++ ) {
    Channel *c = (*_waiters)[i];
    send(c, 0);
  }
  _waiters->clear();

}

void
ConditionVar::notify()
{

  // wake the first guys
  if( _waiters->size() > 0 ) {
    Channel *c = _waiters->front();
    send(c, 0);
    _waiters->erase(&(_waiters->front()));
  }

}
