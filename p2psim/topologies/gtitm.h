/*
 * Copyright (c) 2003-2005 Frank Dabek
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

#ifndef __GTITM_H
#define __GTITM_H

#include <string>
#include "p2psim/p2psim.h"
#include "p2psim/topology.h"

extern "C" {
#include "gb_graph.h"
#include "gb_save.h"
#include "gb_dijk.h"
};

class gtitm : public Topology {
public:
  gtitm(vector<string>*);
  ~gtitm();
  
  virtual void parse(ifstream&);
  virtual Time latency(IPAddress, IPAddress, bool);
  void swap ();

private:
  hash_map<int, int> memo;
  char _filename[64];
  char _filename_alt[64];
  int _num;
  Graph *g;
};

#undef dist
#endif //  __gTITm_H


