/*
 * Copyright (c) 2003 Frank Dabek (fdabek@mit.edu)
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

class VivaldiNode : public P2Protocol {
public:
  VivaldiNode(IPAddress);
  virtual ~VivaldiNode();

  //from vivaldi.h
  struct Coord {
    vector<double> _v;
    void init2d (double x, double y) {_v.clear (); _v.push_back (x); _v.push_back (y); };
    Coord () {};
    int dim () {return _v.size();};
    Coord (uint d) {for (uint i=0;i<d;i++) _v.push_back(0.0);};
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
  Coord my_location() { return _c; }
  double my_error () { return _pred_err; }
  Coord real_coords ();
  //end vivaldi.h

protected:

  //from vivaldi.h
  int _nsamples; // how many times sample() has been called
  int _dim; //dimensionality of the fit space
  int _adaptive; //use adaptive timestep?
  double _timestep; //minimum timestep
  double _curts;
  double _pred_err; // running average of prediction error
  int _window_size;

  Coord _c; // current estimated coordinates
  vector<Sample> _samples;

  double randf() { return (random()%1000000000) / 1000000000.0; }
  Sample wrongest(vector<Sample> v);
  Sample lowest_latency(vector<Sample> v);
  Coord net_force(Coord c, vector<Sample> v);
  Coord net_force1(Coord c, vector<Sample> v);
  vector<double> get_weights (vector<Sample> v);
  void update_error (vector<Sample> v);

  virtual void algorithm(Sample); // override this
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
      assert (t);
      cerr << "RTT from " << ip () << " to " << dst << " " << now () - before << endl;
      sample (dst, t->my_location(), t->my_error (), (now () - before));
    }
    delete t;
    return ok;
  }


};


//COORD implementation
inline double
dist(VivaldiNode::Coord a, VivaldiNode::Coord b)
{
  double d = 0.0;
  assert (a._v.size () == b._v.size ());
  for (unsigned int i = 0; i < a._v.size (); i++) 
    d += (a._v[i] - b._v[i])*(a._v[i] - b._v[i]);

  return sqrt(d);
}

inline VivaldiNode::Coord
operator-(VivaldiNode::Coord a, VivaldiNode::Coord b)
{
  VivaldiNode::Coord c;
  assert (a._v.size () == b._v.size ());
  for (unsigned int i = 0; i < a._v.size (); i++) 
    c._v.push_back (a._v[i] - b._v[i]);
  return c;
}

inline VivaldiNode::Coord
operator+(VivaldiNode::Coord a, VivaldiNode::Coord b)
{
  VivaldiNode::Coord c;
  assert (a._v.size () == b._v.size ());
  for (unsigned int i = 0; i < a._v.size (); i++) 
    c._v.push_back(a._v[i] + b._v[i]);

  return c;
}

inline VivaldiNode::Coord
operator/(VivaldiNode::Coord c, double x)
{
  for (unsigned int i = 0; i < c._v.size (); i++) 
    c._v[i] /= x;
  return c;
}

inline VivaldiNode::Coord
operator*(VivaldiNode::Coord c, double x)
{
  for (unsigned int i = 0; i < c._v.size (); i++) 
    c._v[i] *= x;
  return c;
}

inline double
length(VivaldiNode::Coord c)
{
  double l = 0.0;
  for (unsigned int i = 0; i < c._v.size (); i++) 
    l += c._v[i]*c._v[i];
  return sqrt(l);
}

ostream& operator<< (ostream &s, VivaldiNode::Coord &c);

#endif // __PROTOCOL_H
