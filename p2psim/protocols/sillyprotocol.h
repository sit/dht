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

#ifndef __SILLY_PROTOCOL_H
#define __SILLY_PROTOCOL_H

#include "p2psim/p2protocol.h"
using namespace std;

class SillyProtocol : public P2Protocol {
public:
  SillyProtocol(IPAddress, Args);
  ~SillyProtocol();

  string proto_name() { return "SillyProtocol"; }
  virtual void join(Args*);
  virtual void lookup(Args*);
  virtual void initstate();
  virtual void nodeevent (Args *) {};

  struct silly_args {
  };

  struct silly_result {
  };

  void be_silly(silly_args*, silly_result*);

private:
  IPAddress _otherguy;
};

#endif // __SILLY_PROTOCOL_H
