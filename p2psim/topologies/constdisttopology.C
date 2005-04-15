/*
 * Copyright (c) 2003-2005 Thomer M. Gil
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
#include "constdisttopology.h"
#include <iostream>
using namespace std;

ConstDistTopology::ConstDistTopology(vector<string>*)
{
}

ConstDistTopology::~ConstDistTopology()
{
}

Time
ConstDistTopology::latency(IPAddress ip1, IPAddress ip2, bool reply)
{
  return _latency;
}


void
ConstDistTopology::parse(ifstream &ifs)
{
  string line;

  while(getline(ifs,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // size delay
    unsigned size = (unsigned) atoi(words[0].c_str());
    _latency = (Time) atoi(words[1].c_str());

    // create the nodes
    for(unsigned ipaddr = 1; ipaddr <= size; ipaddr++) {
      Node *p = ProtocolFactory::Instance()->create((IPAddress) ipaddr);
      send(Network::Instance()->nodechan(), &p);
    }
    break;
  }
}
