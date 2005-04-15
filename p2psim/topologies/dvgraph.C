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

#include "dvgraph.h"
#include <stdio.h>
#include <iostream>
#include <assert.h>
using namespace std;

DVGraph::DVGraph()
  : _n(0), _links(0), _initialized(0), _routes(0)
{
}

DVGraph::~DVGraph()
{
  if(_links)
    free(_links);
  if(_routes)
    free(_routes);
}

// return the latency along the path from n1 to n2.
Time
DVGraph::latency(IPAddress a, IPAddress b, bool reply)
{
  dv();

#if 0
  assert(_ip2i.find(a) != _ip2i.end());
  assert(_ip2i.find(b) != _ip2i.end());
#endif

  int i = _ip2i[a];
  int j = _ip2i[b];
  assert(i < _n && j < _n && i >= 0 && j >= 0);

  int m = routes(i, j)._metric;
  assert(m >= 0 && m < 30000);

  return m;
}

void
DVGraph::add_node(IPAddress a)
{
  if(_ip2i.find(a) != _ip2i.end()){
    cerr << "DVGraph: node with ip=" << a << " already added!" << endl;
    exit(1);
  }
  _ip2i[a] = _n;
  _i2ip.push_back(a);
  assert(_i2ip[_n] == a);
  _n++;
}

// compute routes between all nodes.
void
DVGraph::dv()
{
  int i, j;

  if(_initialized)
    return;
  _initialized = 1;

  _routes = (Entry *) malloc(sizeof(Entry) * _n * _n);
  assert(_routes);
  for(i = 0; i < _n; i++){
    for(j = 0; j < _n; j++){
      routes(i, j)._next = -1;
      routes(i, j)._metric = 30000; // infinity
      routes(i, j)._hops = 30000;
    }
    routes(i, i)._next = i;
    routes(i, i)._metric = 0;
    routes(i, i)._hops = 0;
  }

  // run distance-vector until it converges.
  int iters;
  for(iters = 0; ; iters++){
    bool done = true;
    for(i = 0; i < _n; i++){
      for(j = 0; j < _n; j++){
        int m = links(i, j);
        if(m >= 0){
          // there is a link from i to j with metric m.
          // send i's routing table to j.
          int k;
          for(k = 0; k < _n; k++){
            if(routes(i, k)._next != -1 &&
               routes(i, k)._metric + m < routes(j, k)._metric){
              routes(j, k)._next = i;
              routes(j, k)._metric = routes(i, k)._metric + m;
              routes(j, k)._hops = routes(i, k)._hops + 1;
#if 0
              printf("%d to %d via %d metric %d\n",
                     j, k, i, routes(j, k)._metric);
#endif
              done = false;
            }
          }
        }
      }
    }
    if(done)
      break;
  }

  // check that the network is connected.
  // and find the average path length in milliseconds.
  double hop_sum = 0;
  double metric_sum = 0;
  double neighbors_sum = 0;
  for(i = 0; i < _n; i++){
    for(j = 0; j < _n; j++){
      if(routes(i, j)._next == -1){
        fprintf(stderr, "DVGraph: not connected!\n");
        exit(1);
      }
      metric_sum += routes(i, j)._metric;
      hop_sum += routes(i, j)._hops;
      if(routes(i, j)._next == j && i != j)
        neighbors_sum += 1;
    }
  }
  fprintf(stderr, "DVGraph: %d iters, avg metric %.1f, hops %.1f, degree %.1f\n",
          iters,
          metric_sum / (_n * _n),
          hop_sum / (_n * _n),
          neighbors_sum / _n);
}
