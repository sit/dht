/*
 * Copyright (c) 2003 Robert Morris
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

#include "kelips.h"
#include "p2psim/network.h"
#include <stdio.h>
#include <math.h>
#include <iostream>
#include <map>
using namespace std;

Kelips::Kelips(Node *n, Args a)
  : P2Protocol(n)
{
  _k = a.nget<unsigned>("k", 20, 10);
}

Kelips::~Kelips()
{
}

void
Kelips::join(Args *a)
{
  IPAddress wkn = a->nget<IPAddress>("wellknown");
  if(wkn != ip()){
    gotinfo(Info(wkn));
  }
  cout << "Kelips::join ip=" << ip() << " wkn=" << wkn << "\n";
}

void
Kelips::leave(Args *a)
{
}

void
Kelips::crash(Args *a)
{
}

void
Kelips::lookup(Args *a)
{
}

void
Kelips::insert(Args *a)
{
}

// This node has just learned about another node.
// Remember the information, prepare to gossip it.
void
Kelips::gotinfo(Info i)
{
  _info[i._ip] = i;
}
