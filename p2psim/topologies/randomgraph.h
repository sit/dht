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

#ifndef __RANDOMGRAPH_H
#define __RANDOMGRAPH_H

// This Topology simulates a random graph topology among
// the Nodes. That is, every Node has a few links to other
// Nodes, with randomly chosen link latencies. RandomGraph
// computes shortest paths through the graph in order
// to find latencies between Nodes that aren't directly
// connected. RandomGraph makes no attempt to embed the
// Nodes in a plane or any other space. The point is to
// test out Vivaldi &c in a situation far removed from
// Euclidean.

#include "dvgraph.h"
#include <string>
using namespace std;

class RandomGraph : public DVGraph {
public:
  RandomGraph(vector<string>*);
  ~RandomGraph();
  
  virtual void parse(ifstream&);

private:
  static const int degree = 5;       // links per node
  static const int maxlatency = 200; // link latency 0..maxlatency
};

#endif //  __RANDOMGRAPH_H
