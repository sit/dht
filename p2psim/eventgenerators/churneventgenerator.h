/*
 * Copyright (c) 2003 Thomer M. Gil (thomer@csail.mit.edu)
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

#ifndef __CHURN_EVENT_GENERATOR_H
#define __CHURN_EVENT_GENERATOR_H

#include "p2psim/eventgenerator.h"
#include "p2psim/p2psim.h"
#include "p2psim/args.h"
using namespace std;

class ChurnEventGenerator : public EventGenerator {
public:
  ChurnEventGenerator(Args *);
  virtual void kick(Observed *, ObserverInfo*);
  virtual void run();

private:
  IPAddress _wkn;
  string _wkn_string;
  string _proto;
  unsigned _lifemean;
  unsigned _deathmean;
  unsigned _lookupmean;
  string _exittime_string;
  Time _exittime;
  unsigned long _seed;

  Time next_exponential( uint mean );
  string get_lookup_key();

};

#endif // __CHURN_EVENT_GENERATOR_H

