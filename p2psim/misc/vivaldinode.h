/*
 * Copyright (c) 2003-2005 Frank Dabek (fdabek@mit.edu)
 *                    Robert Morris (rtm@csail.mit.edu).
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

#ifndef __VIVNODE_H
#define __VIVNODE_H

#include "p2psim/args.h"
#include "p2psim/node.h"
#include "p2psim/p2protocol.h"
#include <assert.h>
#include <stdio.h>

#define MODEL_SPHERE 2
#define MODEL_EUCLIDEAN 3
#define MODEL_TORUS 4

#define ALG_VIVALDI 1
#define ALG_SIMPLEX 2
#define ALG_PERFECT 3

class VivaldiNode : public P2Protocol {
public:
  VivaldiNode(IPAddress);
  virtual ~VivaldiNode();

  //from vivaldi.h
  struct Coord {
    vector<double> _v;
    double _ht;

    int dim () {return _v.size();};
    double& operator[] (int i) { return _v[i]; };

    void init2d (double x, double y) {_ht = 0; _v.clear (); _v.push_back (x); _v.push_back (y); };
    Coord () { _ht = 0;};
    Coord (uint d) { _ht = 0; _v.clear (); for (uint i=0;i<d;i++) _v.push_back(0.0);};
  };

  struct Sample {
    Coord _c;
    double _latency;
    IPAddress _who;
    double _error;
    Sample(Coord c, double l, double e, IPAddress w) { 
      _c = c; _latency = l; _who = w; _error = e;
    }
  };
  
  int nsamples() { return _nsamples; }
  void sample(IPAddress who, Coord c, double e, double latency);
  Coord my_location();
  double my_error () { return _pred_err; }
  Coord real_coords ();
  //end vivaldi.h

  //from vivaldi.h
  int _nsamples; // how many times sample() has been called
  uint _dim; //dimensionality of the fit space
  int _adaptive; //use adaptive timestep?
  double _timestep; //minimum timestep
  double _curts;
  double _pred_err; // running average of prediction error
  uint _window_size;
  string _model;
  int _model_type;
  double _radius;
  int _initial_triangulation;
  unsigned int _num_init_samples;
  double _tsize;
  string _algorithm;
  int _algorithm_type;

  Coord _c; // current estimated coordinates
  vector<Sample> _samples;
  vector<Sample> _init_samples;

  double randf() { return (random()%1000000000) / 1000000000.0; }
  Sample wrongest(vector<Sample> v);
  Sample lowest_latency(vector<Sample> v);
  Coord net_force(Coord c, vector<Sample> v);
  Coord net_force1(Coord c, vector<Sample> v);
  Coord net_force_toroidal(Coord c, vector<Sample> v);
  vector<double> get_weights (vector<Sample> v);
  void update_error (vector<Sample> v);
  double dist(VivaldiNode::Coord a, VivaldiNode::Coord b);
  double spherical_dist (VivaldiNode::Coord a, VivaldiNode::Coord b);
  double toroidal_dist (VivaldiNode::Coord a, VivaldiNode::Coord b);
  double spherical_dist_arc (VivaldiNode::Coord a, VivaldiNode::Coord b);
  virtual void algorithm(Sample); // override this
  void initial_triangulation (vector<Sample> isamps);
  //end vivaldi.h


  template<class BT, class AT, class RT>
  bool doRPC(IPAddress dst, void (BT::* fn)(AT *, RT *), AT *args, RT *ret)
  {
    assert (dst > 0);
    
    Thunk<BT, AT, RT> *t = _makeThunk(dst, dynamic_cast<BT*>(getpeer(dst)), fn, args, ret);
    Time before = now ();
    bool ok = _doRPC(dst, &Thunk<BT, AT, RT>::thunk, (void *) t);
    if (ok) {
      VivaldiNode * t = dynamic_cast<VivaldiNode *>(getpeer(dst));
      Topology *topo = Network::Instance ()->gettopology ();
      assert (t);
      //      cerr << ip() << " " << dst << " " << now () - before << endl;
      if (topo->valid_latency (ip (), dst))
	sample (dst, t->my_location(), t->my_error (), (now () - before));
    }
    delete t;
    return ok;
  }
};

double dist(VivaldiNode::Coord, VivaldiNode::Coord);
VivaldiNode::Coord operator-(VivaldiNode::Coord, VivaldiNode::Coord);
VivaldiNode::Coord operator+(VivaldiNode::Coord, VivaldiNode::Coord);
VivaldiNode::Coord operator/(VivaldiNode::Coord, double);
VivaldiNode::Coord operator*(VivaldiNode::Coord, double);
double length(VivaldiNode::Coord);
double plane_length(VivaldiNode::Coord);
ostream& operator<<(ostream&, VivaldiNode::Coord&);
#endif // __PROTOCOL_H
