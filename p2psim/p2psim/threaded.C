/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu)
 *                    Robert Morris (rtm@csail.mit.edu)
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
