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
#include <stdio.h>
#include <iostream>
using namespace std;

vector<VivaldiTest*> VivaldiTest::_all;

VivaldiTest::VivaldiTest(Node *n, Args &args)
  : DHTProtocol(n), _next_neighbor(0), _neighbors(0)
{
  _vo = args.nget<int>("vivaldi-algorithm", 10);
  _dim = args.nget<int>("model-dimension", 10);
  if (_dim < 0) {
    cerr << "dimension must be specified (and positive)\n";
    exit (0);
  }

  int timestep_scaled = args.nget<int>("timestep", 1000, 10);
  _timestep = (double)timestep_scaled/1000000.0;
  _adaptive = atoi ((args)["adaptive"].c_str());
  _neighbors = args.nget<int>("neighbors", 16);
}

VivaldiTest::~VivaldiTest()
{
}

void
VivaldiTest::join(Args *args)
{


  switch(_vo){
  case 1: _vivaldi = New Vivaldi1(node(),_dim); break;
  case 2: _vivaldi = New Vivaldi2(node(),_dim); break;
  case 3: _vivaldi = New Vivaldi3(node(),_dim); break;
  case 4: _vivaldi = New Vivaldi4(node(),_dim); break;
  case 5: _vivaldi = New Vivaldi5(node(),_dim); break;
  case 6: _vivaldi = New Vivaldi6(node(),_dim); break;
  case 7: _vivaldi = New Vivaldi7(node(),_dim); break;
  case 8: _vivaldi = New Vivaldi8(node(),_dim); break;
  case 9: _vivaldi = New Vivaldi9(node(),_dim); break;
  case 10: {
#if 0
    cout << ip() << " joined: " << " t = " << _timestep << "\n";
#endif
    _vivaldi = New Vivaldi10(node(), _dim, _timestep, _adaptive); 
    break;
  }
  default:
    fprintf(stderr, "VivaldiTest: bad Vivaldi algorithm %s\n",
            (*args)["vivaldi-algorithm"].c_str());
    exit(1);
  }
  
  _all.push_back(this);
  
  addNeighbors ();

  delaycb(1000, &VivaldiTest::tick, (void *) 0);
}

void
VivaldiTest::addNeighbors ()
{
  if (_old_all_size == _all.size ()) 
    return;
    
  _nip.clear ();
  
  uint next_index = node()->ip() + 1;
  //  cerr << node()->ip() << "'s neighbors are ";
  while ((int)_nip.size () < _neighbors) {
    if (next_index >= _all.size ()) next_index = 0;
    _nip.push_back(_all[next_index]->node()->ip());
    //    cerr << _nip.back () << " ";
    next_index++;
  }
  //  cerr << "\n";
  _old_all_size = _all.size ();
}
char *
VivaldiTest::ts()
{
  static char buf[50];
  sprintf(buf, "%llu Vivaldi(%u)", now(), node()->ip());
  return buf;
}

Vivaldi::Coord
VivaldiTest::real()
{
  Vivaldi::Coord c;
  Topology *t = Network::Instance()->gettopology();
  if(Euclidean *xt = dynamic_cast<Euclidean*>(t)){
    Euclidean::Coord rc = xt->getcoords(node()->ip());
    c.init2d(rc.first, rc.second);
  } else  if(EuclideanGraph *xt = dynamic_cast<EuclideanGraph*>(t)){
    EuclideanGraph::Coord rc = xt->getcoords(node()->ip());
    c.init2d(rc._x, rc._y);
  } else {
    c.init2d(0,0);
  }
  return c;
}

// Calculate this node's error: average error in distance
// to each other node.
double
VivaldiTest::error()
{
  Topology *t = (Network::Instance()->gettopology());
  double sum = 0;
  uint sum_sz = 0;
  Vivaldi::Coord vc = _vivaldi->my_location();
  for(unsigned i = 0; i < _all.size(); i++){
    Vivaldi::Coord vc1 = _all[i]->_vivaldi->my_location();
    double vd = dist(vc, vc1);
    double rd = t->latency(node()->ip(), _all[i]->node()->ip());
    sum += fabs(vd - rd);
    if (_vivaldi->nsamples() % 500 == 0)
      printf ("pair_error %d --> %d : %f %f\n", ip(), _all[i]->ip(), vd, rd);
    sum_sz++;
  }
  return (sum / sum_sz);
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
    Vivaldi::Coord vc = _all[i]->_vivaldi->my_location();
    for (uint j = 0; j < n; j++) {
      if (i != j) {
	Vivaldi::Coord vc1 = _all[j]->_vivaldi->my_location();
	//	double e = _all[i]->error();
	double vd = dist(vc, vc1);
	double rd = t->latency(_all[i]->node()->ip(), _all[j]->node()->ip());
	double e = fabs(vd - rd);
	a.push_back(e);
	errpts++;
	if (_vivaldi->nsamples () % 500 == 0 && _vivaldi->nsamples () > 0) 
	  // on special occassions, print all errors
	  printf ("pair_error %d --> %d : %f %f\n", 
		  i, j, vd, rd);
      }
    }
  }

  sort(a.begin(), a.end());
  if(n > 5){
    e05 = a[errpts / 20];
    e50 = a[errpts / 2];
    e95 = a[errpts - (errpts / 20)];
  } else if(n > 0) {
    e05 = a[0];
    e50 = a[errpts / 2];
    e95 = a[errpts-1];
  }
}

void
VivaldiTest::print_all_loc()
{
  unsigned int n = _all.size();
  Vivaldi::Coord vc;
  for (uint i = 0; i < n; i++) {
    vc = _all[i]->_vivaldi->my_location();
    printf("COORD %d %u: ", (int) _all[i]->ip(), (unsigned)now ());
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
    printf("# %s nnodes=%d %s neighbors=%d\n",
           typeid(*(this->_vivaldi)).name(),
           _all.size(),
           typeid(*(Network::Instance()->gettopology())).name(),
           _neighbors);
  }

  Vivaldi::Coord rc = real();
  Vivaldi::Coord vc = _vivaldi->my_location();
  double e05, e50, e95;
  total_error(e05, e50, e95);
  printf("vivaldi %u %u %d %.5f %.5f %.5f\n",
         (unsigned) now(),
         node()->ip(),
         _vivaldi->nsamples(),
         e05,
         e50,
         e95);

  fflush(stdout);
}

void
VivaldiTest::tick(void *)
{
  addNeighbors ();
  IPAddress dst;
  if(_neighbors > 0){
        dst = _nip[random() % _neighbors];
    // dst = _nip[_next_neighbor++ % _nip.size()];
  } else {
    dst = _all[random() % _all.size()]->node()->ip();
  }

#if 0
  if(node()->ip() == 1) {
    status();
    print_all_loc();
  }
#endif

  Vivaldi::Coord c;
  _vivaldi->doRPC(dst,
                  dynamic_cast<VivaldiTest*>(getpeer(dst)),
                  &VivaldiTest::handler,
                  (void *) 0, &c);


  delaycb(1000, &VivaldiTest::tick, (void *) 0);
}

void
VivaldiTest::handler(void *args, Vivaldi::Coord *ret)
{
  *ret = _vivaldi->my_location();
}








