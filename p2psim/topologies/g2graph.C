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

#include "g2graph.h"
#include "p2psim/topology.h"
#include "p2psim/network.h"
#include "protocols/protocolfactory.h"
#include <iostream>
using namespace std;

/* the Gummadi brothers' graph */
G2Graph::G2Graph(vector<string> *v)
{
  _num = atoi((*v)[0].c_str());
  assert(_num > 0);

  _latmap.clear();
  _samples.clear();
}

G2Graph::~G2Graph()
{
}

void
G2Graph::parse(ifstream &ifs)
{
  string line;
  string distfile;
  while (getline(ifs,line)) {
    vector<string> words = split(line);
    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    if (words.size() == 2) {
      if (words[0] != "latencydistribution") {
	cerr << "wrong G2Graph format" << endl;
      }
      distfile = words[1];
      break;
    }
  }

  //get sample distribution
  ifstream in(distfile.c_str());
  while (getline(in,line)) {
    int lat = atoi(line.c_str());
    assert(lat > 0);
    _samples.push_back(lat);
  }
  sort(_samples.begin(), _samples.end());
  _med_lat = _samples[_samples.size()/2];

  //create nodes
  for (uint ipaddr = 1; ipaddr <= _num; ipaddr++) {

    // what kind of node?
    Node *p = ProtocolFactory::Instance()->create(ipaddr);

    // add the node to the network
    send(Network::Instance()->nodechan(), &p);
  }
} 

Time
G2Graph::latency(IPAddress ip1x, IPAddress ip2x, bool reply)
{
  IPAddress ip1 = Network::Instance()->first_ip(ip1x);
  IPAddress ip2 = Network::Instance()->first_ip(ip2x);
  if (ip1 == ip2) return 0;

  //samples a random latency from samples
  //remembers it so future latencies for this pair will remain the same
  //also ensure symmetry
  pair<IPAddress, IPAddress>  p;
  if (ip1 < ip2) {
    p.first = ip1;
    p.second = ip2;
  }else{
    p.first = ip2;
    p.second = ip1;
  }

  if (_latmap.find(p) != _latmap.end()) {
    return _latmap[p];
  }else{

    //sample
    double r = (double)random()/(double)RAND_MAX;
    uint i = (uint) (r * _samples.size());
    _latmap[p] = _samples[i];
    return _latmap[p];
  }
}
