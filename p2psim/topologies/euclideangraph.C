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

#include "p2psim/network.h"
#include "p2psim/parse.h"
#include "protocols/protocolfactory.h"
#include "euclideangraph.h"
#include <stdio.h>
#include <math.h>
#include <iostream>
using namespace std;

EuclideanGraph::EuclideanGraph(vector<string>*)
{
}

EuclideanGraph::~EuclideanGraph()
{
}

void
EuclideanGraph::parse(ifstream &ifs)
{
  string line;
  while(getline(ifs,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // nodeid and coordinates
    if(words.size() < 2) {
      cerr << "EuclideanGraph: provide nodeid and coordinates per line" << endl;
      exit(1);
    }

    // node-id
    IPAddress ipaddr = atoi(words[0].c_str());
    if(!ipaddr)
      cerr << "found node-id 0.  you're asking for trouble." << endl;

    // x,y coordinates
    vector<string> coords = split(words[1], ",");
    Coord c;
    c._x = atof(coords[0].c_str());
    c._y = atof(coords[1].c_str());

    // add the node to the network
    Node *p = ProtocolFactory::Instance()->create(ipaddr);
    send(Network::Instance()->nodechan(), &p);

    // remember the new node in DVGraph's tables
    add_node(p->ip());

    // remember the node's coordinates
    _coords.push_back(c);
  }

  // Initialize links.
  int i, j;
  _links = (short *) malloc(sizeof(short) * _n * _n);
  assert(_links);
  for(i = 0; i < _n; i++)
    for(j = 0; j < _n; j++)
      links(i, j) = -1;

  // Generate some random links to/from each node.
  for(i = 0; i < _n; i++){
    Coord c1 = _coords[i];
    for(j = 0; j < degree; j++){
      int k = random() % _n;          // pick a random node.
      Coord c2 = _coords[k];
      int m = (int) hypot(c2._x - c1._x, c2._y - c1._y);
      links(i, k) = m;
      links(k, i) = m;
    }
  }

  // Guess what the likely Vivaldi errors will be.
  // That is, the difference between direct Euclidean distance
  // and latency over the shortest path.
  double sum = 0;
  for(i = 0; i < _n; i++){
    Coord c1 = _coords[i];
    for(j = 0; j < _n; j++){
      Coord c2 = _coords[j];
      double d = hypot(c2._x - c1._x, c2._y - c1._y);
      Time lat = latency(_i2ip[i], _i2ip[j]);
      sum += fabs(d - lat);
    }
  }
  fprintf(stderr, "EuclideanGraph: typical Vivaldi error %.1f\n",
          sum / (_n * _n));
}

EuclideanGraph::Coord
EuclideanGraph::getcoords(IPAddress a)
{
  unsigned int i = _ip2i[a];
  assert(i >= 0 && i < _coords.size() && _i2ip[i] == a);
  return _coords[i];
}
