/*
 * Copyright (c) 2003 Robert Morris, Frank Dabek (rtm, fdabek)@mit.edu
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
  _neighbors = args.nget<int>("neighbors", 16, 10);
  _total_nodes = args.nget<int>("totalnodes", -1, 10);
  _vis = args.nget<int>("vis", 0, 10);

  if (_total_nodes < 0) {
    cerr << "totalnodes parameter not optional\n";
    exit (-1);
  }
  _ticks = 0;
}

VivaldiTest::~VivaldiTest()
{
}


void
VivaldiTest::join(Args *args)
{
  if ((int)_all.size () > _total_nodes && _total_nodes > 0) 
      return;
  _all.push_back(this);
  addNeighbors ();
  if (_vis && !init_state ()) cerr << "vis " << now () << " node " << ip () << " " << _c << "\n";
  delaycb(1000, &VivaldiTest::tick, (void *) 0);
}


void
VivaldiTest::initstate () 
{
  Topology *t = Network::Instance ()->gettopology ();
  Euclidean *e = dynamic_cast<Euclidean *>(t);
  if (e) {
    pair<int, int> c = e->getcoords (ip ());
    _c.init2d ((double)(c.first), (double)(c.second));
    if (_vis) cerr << "vis 0 node " << ip () << " " << _c << "\n";
  }
}

void
VivaldiTest::tick(void *)
{
  _ticks++;
  IPAddress dst;
  if(_neighbors > 0){
    if (random () % 2 == 0 && _nip_best.size () > 0 && false)
      dst = _nip_best[random () % _nip_best.size ()];
    else
      dst = _nip[random() % _neighbors];

  } else {
    dst = _all[random() % _all.size()]->ip();
  }

  if(this->ip() == 1) {

    if (_ticks % 5 == 0) 
      {
	status();
	//update neighbors
	if (_neighbors > 0) {
	  vector<IPAddress> best = best_n (_neighbors);
	  for(unsigned i = 0; i < _all.size(); i++) 
	    _all[i]->_nip_best = best;
	}
      }

    if (_ticks % 100 == 0 || _vis) print_all_loc();
  }

  //see if our dest has joined
  Node *n = getpeer (dst);
  if (!n) return;

  doRPC(dst, &VivaldiTest::handler, (void *) 0, (void *)0);
  if (_vis) {
    cout << "vis " << now () << " step " 
	 << ip () << " 0 " << (int)error () << " ";
    Coord loc = my_location ();
    for (int j = 0; j < loc.dim(); j++)
      cout << (int)loc._v[j] << " ";
    cout << endl;
  }
  delaycb(1000, &VivaldiTest::tick, (void *) 0);
}

void
VivaldiTest::handler(void *args, void *ret)
{
}


void
VivaldiTest::addNeighbors ()
{
  _nip.clear ();
  
  cerr << "neighbors for " << this->ip() << " ";
  while ((int)_nip.size () < _neighbors) {
    int cand = -1;
    while (cand < 0 || cand > _total_nodes)
      cand = (random () % (_total_nodes)) + 1;

    _nip.push_back(cand);
    //_nip.push_back(_all[random () % _all.size ()]->ip());
    cerr << cand << " ";
  }
  cerr << "\n";
}


bool
comp (pair<double, IPAddress> a, pair<double, IPAddress> b) 
{
  if (a.first < b.first) return true;
  if (a.first > b.first) return false;
  return false;
} 

vector<IPAddress>
VivaldiTest::best_n(unsigned int n)
{
  vector<pair<double, IPAddress> > errs;
  for(unsigned i = 0; i < _all.size(); i++)
    errs.push_back ( pair<double, IPAddress>(_all[i]->error (),_all[i]->ip ()));

  sort(errs.begin(), errs.end(), &comp);

  vector<IPAddress> ret;
  for (unsigned int i = 0; i < n && i < errs.size (); i++) {
    ret.push_back (errs[i].second);
  }
  return ret;
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

void
VivaldiTest::node_errors (double &e05, double &e50, double &e95)
{
  vector<double> a;
  for (unsigned int i = 0; i < _all.size (); i++) 
    a.push_back (_all[i]->error ());
  
  sort(a.begin(), a.end());
  if(a.size () > 5){
    e05 = a[a.size () / 20];
    e50 = a[a.size () / 2];
    e95 = a[a.size () - (a.size() / 20)];
  }
}

// return 5th, 50th, 95th percentiles of per-link errors
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
	//vivaldi predicts ROUND-TRIP distances
	double vd = dist(vc, vc1);
	double rd = 2*t->latency(_all[i]->ip(), _all[j]->ip());
	double e = fabs(vd - rd);
	//	cerr << "error "  << vd << " " << rd << " " << e << "\n";
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
    printf ("%d ", (int) _all[i]->ip());
    for (uint j = 0; j < vc._v.size(); j++)
      printf ("%d ", (int)vc._v[j]);
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
  // total_error(e05, e50, e95);
  node_errors (e05, e50, e95);
  
  printf("vivaldi %u %u %d %.5f %.5f %.5f %.5f\n",
         (unsigned) now(),
         this->ip(),
         _ticks,
         e05,
         e50,
         e95,
	 my_error ());

  fflush(stdout);
}
