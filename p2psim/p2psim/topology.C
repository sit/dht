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

#include "topologies/topologyfactory.h"
#include "failuremodels/failuremodelfactory.h"
#include "network.h"
#include "parse.h"
#include <iostream>

using namespace std;

Topology::Topology()
{
  _med_lat = 0;
  _lossrate = 0;
  _noise = 0;
}

Topology::~Topology()
{
}

string
Topology::get_node_name(IPAddress ip)
{
  char buf[10];
  sprintf(buf,"%u",ip);
  return string(buf);
}

Topology*
Topology::parse(char *filename)
{
  extern bool with_failure_model;

  ifstream in(filename);
  if(!in) {
    cerr << "no such file " << filename << endl;
    taskexitall(0);
  }

  string line;
  Topology *top = 0;
  FailureModel *failure_model = 0;
  while(getline(in,line)) {
    vector<string> words = split(line);

    // break on first empty line
    if(words.empty())
      break;

    // skip empty lines and commented lines
    if(words[0][0] == '#')
      continue;

    // topology
    if(words[0] == "topology") {
      words.erase(words.begin());
      string topology = words[0];
      words.erase(words.begin());
      top = TopologyFactory::create(topology, &words);

    // noise
    } else if(words[0] == "noise") {
      if(!top) {
        cerr << "topology keyword must appear before noise keyword" << filename << endl;
	continue;
      }
      top->_noise = (unsigned) (atof(words[1].c_str()) * 100);
      cout << "noise = " << top->noise_variance() << endl;
      assert(top->noise_variance() >= 0);

    // loss_rate
    } else if(words[0] == "loss_rate") {
      if(!top) {
        cerr << "topology keyword must appear before loss_rate keyword" << filename << endl;
	continue;
      }
      top->_lossrate = (unsigned) (atof(words[1].c_str()) * 100);
      assert(top->lossrate() >= 0 && top->lossrate() <= 10000);

    // failure_model
    } else if(words[0] == "failure_model") {
      if(!with_failure_model) {
        cerr << "warning: -f flag but found failure_model keyword. ignoring failure_model!" << filename << endl;
	continue;
      }
      words.erase(words.begin());
      string fm = words[0];
      words.erase(words.begin());
      failure_model = FailureModelFactory::create(fm, &words);
    } else {
      cerr << "header lines in topology file should be ``topology [T]'' or ``failure_model [F]" << endl;
      exit(-1);
    }
  }

  if(!top) {
    cerr << "the topology you specified is unknown" << endl;
    exit(-1);
  }

  if(!failure_model) {
    if (!with_failure_model) {
      vector<string> words;
      failure_model = FailureModelFactory::create("NullFailureModel", &words);
      assert (failure_model);
    } else {
      cerr << "the failure_model you specified is unknown" << endl;
      exit(-1);
    }
  }

  // create the network.
  Network::Instance(top, failure_model);

  // leave the rest of the file to the specific topology
  top->parse(in);

  return top;
}
