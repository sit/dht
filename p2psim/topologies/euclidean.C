/*
 * Copyright (c) 2003 [NAMES_GO_HERE]
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
#include "p2psim/topology.h"
#include "protocols/protocolfactory.h"
#include "euclidean.h"
#include <cmath>
#include <iostream>
using namespace std;

Euclidean::Euclidean(vector<string>*)
{
}

Euclidean::~Euclidean()
{
}

Time
Euclidean::latency(IPAddress ip1, IPAddress ip2, bool reply)
{
  Coord c1 = _nodes[ip1];
  Coord c2 = _nodes[ip2];

  return (Time) hypot(labs(c2.first - c1.first), labs(c2.second - c1.second));
}


void
Euclidean::parse(ifstream &ifs)
{
  string line;
  Time max_first, max_second;

  max_first = max_second = 0;
  while(getline(ifs,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // nodeid, coordinates and at least one protocol
    if(words.size() < 2) {
      cerr << "provide nodeid and coordinates per line" << endl;
      continue;
    }

    // node-id
    IPAddress ipaddr = (IPAddress) strtoull(words[0].c_str(), NULL, 10);
    if(!ipaddr)
      cerr << "found node-id 0.  you're asking for trouble." << endl;

    // x,y coordinates
    vector<string> coords = split(words[1], ",");
    Coord c;
    if (coords.size () < 2) {
      cerr << "malformed coordinates " << endl;
      exit (-1);
    }
    c.first = atoi(coords[0].c_str());
    c.second = atoi(coords[1].c_str());
    if ((Time) c.first > max_first) 
      max_first = c.first;
    else if ((Time) c.second > max_second)
      max_second = c.second;

    // what kind of node?
    Node *p = ProtocolFactory::Instance()->create(ipaddr);

    // add the new node it to the topology
    if(_nodes.find(p->ip()) != _nodes.end())
      cerr << "warning: node " << ipaddr << " already added! (" <<words[0]<<")" << endl;
    _nodes[p->ip()] = c;

    // add the node to the network
    send(Network::Instance()->nodechan(), &p);
  }

  _med_lat = (Time) sqrt((double) (max_first * max_first + max_second * max_second))/3;
}
