/*
 * Copyright (c) 2003-2005 Jinyang Li
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

#ifndef __E2EGRAPH_H
#define __E2EGRAPH_H

#include <string>
#include "p2psim/bighashmap.hh"
using namespace std;


class E2EGraph : public Topology {
public:
  E2EGraph(vector<string>*);
  ~E2EGraph();
  
  typedef pair<unsigned, unsigned> Coord;
  virtual void parse(ifstream&);
  bool valid_latency (IPAddress ip1x, IPAddress ip2x);
  Time latency(IPAddress, IPAddress, bool = false);

private:
  vector<vector<int> > _pairwise;
};

#endif //  __E2EGRAPH_H
