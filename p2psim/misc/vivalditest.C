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
#include <cmath>
using namespace std;

vector<VivaldiTest*> VivaldiTest::_all;

VivaldiTest::VivaldiTest(IPAddress i, Args &args)
  : VivaldiNode(i), _next_neighbor(0), _neighbors(0)
{
  //grid configuration will give each node 4 neighbors 
  //in a square grid-like configuration so the network is 
  //guaranteed to be connected. Only works with 
  //consecutive IP addresses.
  _grid_config = args.nget<int>("grid_config",0,10);
  _ring_config = args.nget<int>("ring_config",0,10);

  _neighbors = args.nget<int>("neighbors", 16, 10);
  _total_nodes = args.nget<uint>("totalnodes", 0, 10);
  _vis = args.nget<int>("vis", 0, 10);
  _far_percent = args.nget<int>("far-percent", 90, 10);
  _ticks = 0;
  _joined = false;
}

VivaldiTest::~VivaldiTest()
{
}


void
VivaldiTest::nodeevent (Args *args)
{
  int _queue = args->nget<int> ("queue", 0, 10);
  if (_queue > 0) 
    queue_delay (_queue);
}

void
VivaldiTest::join(Args *args)
{
  if (_total_nodes == 0) {
    const set<Node*> *l= Network::Instance()->getallnodes();
    _total_nodes = l->size();
  }

  if (_all.size () > _total_nodes && _total_nodes > 0) 
      return;

  if (_initial_triangulation && _init_samples.size () >= _num_init_samples
      || _all.size () < _num_init_samples) {
    _all.push_back(this);
    _joined = true;
  }

  int _queue = args->nget<int> ("queue", 0, 10);
  if (_queue > 0) 
    queue_delay (_queue);

  if (_grid_config) {
    int row = (int) sqrt((double) _total_nodes);
    printf("my ip is %u\n", _ip);
    int nbr_ip;
    
    nbr_ip = (int) ip() - 1 ;
    if (nbr_ip > 0) 
      _nip.push_back(nbr_ip);

    nbr_ip = (int) ip() + 1;
    if (nbr_ip <= (int)_total_nodes) 
      _nip.push_back(nbr_ip);

    nbr_ip = (int) ip() - row;
    if (nbr_ip > 0) 
      _nip.push_back(nbr_ip);

    nbr_ip = (int) ip() + row;
    if (nbr_ip <= (int)_total_nodes)
      _nip.push_back(nbr_ip);


    //add some "distant" nodes to use at some probability
    for (int i = 1; i < 10; i++) {
      uint cand = (random () % (_total_nodes)) + 1;
      assert(cand <= _total_nodes);
      _nip_far.push_back(cand);
      //_nip.push_back(_all[random () % _all.size ()]->ip());
    }
    _neighbors_far = _nip_far.size ();

    _neighbors = _nip.size();
    printf("%u joined with %d neighbors: ",ip(), _nip.size());
    for (uint i = 0; i < _nip.size(); i++) {
      assert(_nip[i] > 0 && _nip[i] <= _total_nodes);
      printf("%u ", _nip[i]);
    }
    printf("\n");
  }else if (_ring_config) {
    //add "successor"
    uint cand = ip () + 1;
    if (cand > _total_nodes) cand = 1;
    _nip.push_back (cand);

    for (int i = 1; i < _neighbors; i++) {
      int dist = 1 << i;
      double r = ((double)random () / (double)RAND_MAX);
      cand = ((int)(ip () + (1+r)*_total_nodes/dist ) % _total_nodes) + 1;
      _nip.push_back (cand);
    }
    
    printf("RC %u joined with %d neighbors: ",ip(), _nip.size());
    for (uint i = 0; i < _nip.size(); i++) {
      printf("%u ", _nip[i]);
      assert(_nip[i] > 0 && _nip[i] <= _total_nodes);
    }
    printf("\n");
  } else {
    addRandNeighbors ();
  }
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

  if (!_joined && (!_initial_triangulation  || 
		   (_initial_triangulation 
		    && _init_samples.size () >= _num_init_samples))) {
    _all.push_back(this);
    _joined = true;
  }

  if(_neighbors > 0){
    if (random () % 2 == 0 && _nip_best.size () > 0 && false)
      dst = _nip_best[random () % _nip_best.size ()];
    else if (_grid_config) {
      double p = (double)random()/(double)RAND_MAX;
      if (p*100 > _far_percent)
	dst = _nip_far[random() % _nip_far.size()];
      else
	dst = _nip[random() % _neighbors];
    } else {
      dst = _nip[random() % _neighbors];
    }
  } else {
    dst = _all[random() % _all.size()]->ip();
  }

  if(this->ip() == 2) {

    if (_ticks % 5 == 0) 
      {
	status();
	//update neighbors
	//	if (_neighbors > 0) {
	///	  vector<IPAddress> best = best_n (_neighbors);
	//  for(unsigned i = 0; i < _all.size(); i++) 
	//    _all[i]->_nip_best = best;
	//	}
      }

    if (_ticks % 5 == 0 || _vis) print_all_loc();
  } else if (false && this->ip () >= 1000)
    {
      cout << this->ip () << " " << error () << "\n";
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
VivaldiTest::addRandNeighbors ()
{
  _nip.clear ();
  
  cerr << "neighbors for " << this->ip() << " ";
  while ((int)_nip.size () < _neighbors) {
    uint cand = (random () % (_total_nodes)) + 1;
    assert(cand <= _total_nodes);
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

// Calculate this node's error: median error in distance
// to each other node.
double
VivaldiTest::error()
{
  Topology *t = (Network::Instance()->gettopology());
  vector<double> a;
  int sum_sz = 0;
  VivaldiNode::Coord vc = my_location();
  for(uint i = 0; i < _all.size(); i++){
    VivaldiNode::Coord vc1 = _all[i]->my_location();
    double vd = dist(vc, vc1);
    double rd = 2*t->latency(this->ip(), _all[i]->ip());
    double e = fabs(vd - rd);
    // double e = fabs(vd - rd)/(rd);
    a.push_back (e);
    sum_sz++;
  }

  sort (a.begin (), a.end ());
  int n = _all.size ();
  if(n > 5)
    _last_error = a[sum_sz/2];
  else
    _last_error = 0;
  return _last_error;
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
    printf ("%d at ",  _all[i]->ip());
    for (uint j = 0; j < vc._v.size(); j++)
      printf ("%s%f", j ? "," : "", vc._v[j]);
    if ((int)vc._ht)
      printf (",ht=%d", (int)vc._ht);
    //  (void)_all[i]->error ();
    printf (" with error %f %f\n", _all[i]->_last_error, _all[i]->my_error ());
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
  //total_error(e05, e50, e95);
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
