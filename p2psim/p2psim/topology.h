/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu)
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

#ifndef __TOPOLOGY_H
#define __TOPOLOGY_H

// abstract super class of a topology
#include <fstream>
#include "p2psim/parse.h"
#include "p2psim.h"
using namespace std;

class Topology {
public:
  // will create the appropriate topology object
  static Topology* parse(char *);
  virtual void parse(ifstream&) = 0;

  virtual Time latency(IPAddress, IPAddress, bool = false) = 0;
  virtual string get_node_name(IPAddress);
  virtual Time median_lat() { return _med_lat; } 
  virtual bool valid_latency (IPAddress ip1x, IPAddress ip2x) { return true;};
  virtual ~Topology();
  unsigned lossrate() { return _lossrate; }
  unsigned noise_variance() { return _noise; }
  unsigned num() { return _num; }

protected:
  Topology();
  Time _med_lat;
  unsigned _lossrate;
  unsigned _noise;
  unsigned int _num;
};

#endif //  __TOPOLOGY_H
