/*
 * Copyright (c) 2003-2005 Russ Cox
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/* $Id: condvar.C,v 1.9 2005/04/15 20:51:26 thomer Exp $ */

#include "condvar.h"
using namespace std;

ConditionVar::ConditionVar()
{
  _waiters = New set<Channel *>;
}

ConditionVar::~ConditionVar()
{
  // TODO: what happens if this gets deleted while there are threads waiting?
  // may have to do something smarter in the future
  //assert( _waiters->size() == 0 );
  delete _waiters;
}

void
ConditionVar::wait()
{

  // create a channel, put it on the queue of waiting threads
  Channel *c = chancreate(sizeof(int*), 0);
  (*_waiters).insert( c );
  recvp(c);
  chanfree(c);
}

void
ConditionVar::wait_noblock(Channel *c)
{

  // put the already created channel on the waiting list
  (*_waiters).insert( c );
  // doesn't block!!
}

void
ConditionVar::notifyAll()
{

  u_int sz = _waiters->size();
  Channel * chans[sz];
  u_int j = 0;

  // wake all the sleeping guys
  for(set<Channel*>::const_iterator i = _waiters->begin(); 
      i != _waiters->end(); ++i) {
    Channel *c = (*i);
    chans[j++] = c;
  }
  _waiters->clear();

  // we wait til after to call send, since otherwise we'll yield and may
  // end up altering waiters while we're still iterating.
  for( u_int i = 0; i < sz; i++ ) {
    send(chans[i], 0);
  }

}

void
ConditionVar::notify()
{

  if(!_waiters->size())
    return;

  Channel *c = *(_waiters->begin());
  send(c, 0);
  _waiters->erase(c);
}
