/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu),
 *                    Robert Morris (rtm@csail.mit.edu).
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

#include "network.h"
#include "observers/observerfactory.h"
#include "eventgenerators/eventgeneratorfactory.h"
#include "protocols/protocolfactory.h"
#include "threadmanager.h"

unsigned p2psim_verbose = 0;

// New plan for crash-free exit:
// Goal:
//   Call destructors before exit() so that they get
//   a chance to print out statistics.
// Rules:
//   After graceful_exit() starts, no other threads will run.
//   Nobody is allowed to call send() or wait or context
//   switch within a destructor.
void
graceful_exit(void*)
{
  delete ObserverFactory::Instance();
  delete Network::Instance(); // deletes nodes, protocols
  delete ThreadManager::Instance();
  delete EventGeneratorFactory::Instance();
  delete ProtocolFactory::Instance();
  delete EventQueue::Instance();
  __tmg_dmalloc_stats();

  taskexitall(0);
}

