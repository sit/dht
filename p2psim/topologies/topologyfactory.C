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

#include "topologyfactory.h"
#include "constdisttopology.h"
#include "euclidean.h"
#include "e2egraph.h"
#include "e2easymgraph.h"
#include "e2elinkfailgraph.h"
#include "e2etimegraph.h"
#include "g2graph.h"
#include "randomgraph.h"
#include "euclideangraph.h"
#ifdef HAVE_LIBGB
#include "gtitm.h"
#endif

Topology *
TopologyFactory::create(string s, vector<string>* v)
{
  Topology *t = 0;

  if(s == "Euclidean")
    t = New Euclidean(v);

  if(s == "RandomGraph")
    t = New RandomGraph(v);

  if(s == "EuclideanGraph")
    t = New EuclideanGraph(v);

  if (s == "E2EGraph")
    t = New E2EGraph(v);

  if (s == "G2Graph")
    t = New G2Graph(v);

#ifdef HAVE_LIBGB
  if (s == "gtitm")
    t = New gtitm (v);
#endif

  if (s == "E2EAsymGraph")
    t = New E2EAsymGraph(v);

  if (s == "E2ETimeGraph")
    t = New E2ETimeGraph(v);

  if (s == "E2ELinkFailGraph")
    t = New E2ELinkFailGraph(v);

  if (s == "ConstDistTopology")
    t = New ConstDistTopology(v);

  return t;
}
