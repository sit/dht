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

#include <typeinfo>
#include "vivalditest.h"
#include "topologies/euclidean.h"
#include "topologies/euclideangraph.h"
#include "misc/vivaldinode.h"
#include "p2psim/network.h"
#include <stdio.h>
#include <iostream>
using namespace std;

vector<VivaldiTest*> VivaldiTest::_all;

VivaldiTest::VivaldiTest(IPAddress i, Args &args)
  : VivaldiNode(i), _next_neighbor(0), _neighbors(0)
{
  _neighbors = args.nget<int>("neighbors", 16);
  _ticks = 0;
}

VivaldiTest::~VivaldiTest()
{
}


void
VivaldiTest::join(Args *args)
{
  _all.push_back(this);
  addNeighbors ();
  delaycb(1000, &VivaldiTest::tick, (void *) 0);
}


void
VivaldiTest::tick(void *)
{
  _ticks++;
  addNeighbors ();
  IPAddress dst;
  if(_neighbors > 0){
        dst = _nip[random() % _neighbors];
    // dst = _nip[_next_neighbor++ % _nip.size()];
  } else {
    dst = _all[random() % _all.size()]->ip();
  }

  if(this->ip() == 1) {
        status();
	if (_ticks % 100 == 0) print_all_loc();
  }


  doRPC(dst, &VivaldiTest::handler, (void *) 0, (void *)0);
  delaycb(1000, &VivaldiTest::tick, (void *) 0);
}

void
VivaldiTest::handler(void *args, void *ret)
{
}


void
VivaldiTest::addNeighbors ()
{
  if (_old_all_size == _all.size ()) 
    return;
    
  _nip.clear ();
  
  uint next_index = this->ip() + 1;
  //  cerr << this->ip() << "'s neighbors are ";
  while ((int)_nip.size () < _neighbors) {
    if (next_index >= _all.size ()) next_index = 0;
    _nip.push_back(_all[random () % _all.size ()]->ip());
    //    cerr << _nip.back () << " ";
    next_index++;
  }
  //  cerr << "\n";
  _old_all_size = _all.size ();
}


// Calculate this node's error: average error in distance
// to each other node.
double
VivaldiTest::error()
{
  Topology *t = (Network::Instance()->gettopology());
  vector<double> a;
  int sum_sz = 0;
  VivaldiNode::Coord vc = my_location();
  for(unsigned i = 0; i < _all.size(); i++){
    VivaldiNode::Coord vc1 = _all[i]->my_location();
    double vd = dist(vc, vc1);
    double rd = 2*t->latency(this->ip(), _all[i]->ip());
    double e = fabs(vd - rd);
    a.push_back (e);
    sum_sz++;
  }

  sort (a.begin (), a.end ());
  int n = _all.size ();
  if(n > 5){
    return a[sum_sz / 2];
  } else
    return 0;
}

// return 5th, 50th, 95th percentiles of node error
void
VivaldiTest::total_error(double &e05, double &e50, double &e95)
{
  unsigned n = _all.size();
  vector<double> a;
  int errpts = 0;

  Topology *t = (Network::Instance()->gettopology());
  for(unsigned i = 0; i < n; i++){
    VivaldiNode::Coord vc = _all[i]->my_location();
    // double ei = _all[i]->error();
    //    printf ("IERR %d %f\n", i, ei);

    for (uint j = 0; j < n; j++) {
      if (i != j) {
	VivaldiNode::Coord vc1 = _all[j]->my_location();
	double vd = dist(vc, vc1);
	double rd = 2*t->latency(_all[i]->ip(), _all[j]->ip());
	double e = fabs(vd - rd);
	a.push_back(e);
	errpts++;
      }
    }
  }

  fflush (stdout);
  sort(a.begin(), a.end());
  if(n > 5){
    e05 = a[errpts / 20];
    e50 = a[errpts / 2];
    e95 = a[errpts - (errpts / 20)];
  }
}

void
VivaldiTest::print_all_loc()
{
  unsigned int n = _all.size();
  VivaldiNode::Coord vc;
  for (uint i = 0; i < n; i++) {
    vc = _all[i]->my_location();
    printf("%d ", (int) _all[i]->ip());
    for (uint j = 0; j < vc._v.size(); j++)
      printf ("%.1f ", vc._v[j]);
    printf ("\n");
  }
}

void
VivaldiTest::status()
{
  static int first = 1;
  if (first) {
    first = 0;
    printf("# nnodes=%d %s neighbors=%d\n",
           _all.size(),
           typeid(*(Network::Instance()->gettopology())).name(),
           _neighbors);
  }

  VivaldiNode::Coord vc = my_location();
  double e05, e50, e95;
  total_error(e05, e50, e95);
  printf("vivaldi %u %u %d %.5f %.5f %.5f\n",
         (unsigned) now(),
         this->ip(),
         _ticks,
         e05,
         e50,
         e95);

  fflush(stdout);
}
