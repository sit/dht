/*
 * Copyright (c) 2003-2005 Thomer M. Gil
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

#include "sillyprotocol.h"
#include <iostream>
using namespace std;

SillyProtocol::SillyProtocol(IPAddress i, Args a) : P2Protocol(i)
{
  // to get the bogus parameter, do this:
  //  0 : default
  // 10 : base in which to interpret the number
  //
  // unsigned bogus = a.nget<unsigned>("bogus_parameter", 0, 10);
}

SillyProtocol::~SillyProtocol()
{
}

void
SillyProtocol::initstate()
{
  // put any "cheat" code here.
  // for example, we may want to initialize our routing tables such that we are
  // immediately in a stabilized state, rather than going through the
  // time-consuming process of joining all the nodes individually.
  //
  // Network::Instance()->getallnodes() returns a const set<Node*>*, i.e., a set
  // of all the Nodes in the system.
  cout << "initstate on " << ip() << endl;
}


void
SillyProtocol::join(Args *args)
{
}

void
SillyProtocol::lookup(Args *args)
{
  // there is an ip parameter in args.  get it.
  IPAddress xip = args->nget<IPAddress>("ip", 1, 10);

  // these are the parameters and the return value for the RPC
  silly_args a;
  silly_result r;

  // do the RPC
  Time before = now();
  doRPC(xip, &SillyProtocol::be_silly, &a, &r);
  Time after = now();

  cout << "RPC latency from ip `" << ip() << "' to `" << xip <<
    "' is " << after - before << " ms." << endl;
}

void
SillyProtocol::be_silly(silly_args *a, silly_result *r)
{
}
