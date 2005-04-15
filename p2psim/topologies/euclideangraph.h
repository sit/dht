/*
 * Copyright (c) 2003-2005 [NAMES_GO_HERE]
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

#ifndef __EUCLIDEANGRAPH_H
#define __EUCLIDEANGRAPH_H

// This Topology puts each node at a point on a Euclidean
// plane, connects the nodes in a random graph, and finds shortest
// paths through the graph. The link latencies are equal to
// the Euclidean distance between the nodes. The intent is to
// mimic the Internet more closely than either Euclidean or RandomGraph.

#include "dvgraph.h"
#include <string>
using namespace std;

class EuclideanGraph : public DVGraph {
public:
  EuclideanGraph(vector<string>*);
  ~EuclideanGraph();
  
  virtual void parse(ifstream&);

  struct Coord {
    double _x;
    double _y;
  };
  Coord getcoords(IPAddress);

private:
  static const int degree = 5;       // links per node

  vector<Coord> _coords;
};

#endif //  __EUCLIDEANGRAPH_H
