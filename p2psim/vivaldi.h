#ifndef __VIVALDI_H
#define __VIVALDI_H

#include <vector>
#include "p2psim.h"
#include "protocol.h"
#include "network.h"
#include "protocolfactory.h"
#include <iostream>

using namespace std;

// Compute Vivaldi synthetic coordinates.
// Protocol-independent: doesn't care where the measurements
// come from.
// Indexed by IPAddress.
// Anyone can create a Vivaldi, call sample() after each
// RPC, and then call my_location().
// Or you can use Vivaldi::RPC.

class Vivaldi {
 public:
  Vivaldi(Node *n, int d);
  virtual ~Vivaldi();
  int nsamples() { return _nsamples; }

  struct Coord {
    vector<double> _v;
    void init2d (double x, double y) {_v.push_back (x); _v.push_back (y); };
    Coord () {};
    int dim () {return _v.size();};
    Coord (uint d) {for (uint i=0;i<d;i++) _v.push_back(0.0);};
  };

  void sample(IPAddress who, Coord c, double latency);
  Coord my_location() { return _c; }

  // Anyone can use this to make an RPC and have Vivaldi time it.
  template<class BT, class AT, class RT>
    bool doRPC(IPAddress dst, BT *target, void (BT::*fn)(AT*, RT*),
               AT *args, RT *ret);

 protected:
  Node *_n; // this node
  int _nsamples; // how many times sample() has been called
  int _dim; //dimensionality of the fit space
  Coord _c; // current estimated coordinates
  struct Sample {
    Coord _c;
    double _latency;
    Sample(Coord c, double l) { _c = c; _latency = l; }
  };
  vector<Sample> _samples;

  double randf() { return (random()%1000000000) / 1000000000.0; }
  Sample wrongest(vector<Sample> v);
  Sample lowest_latency(vector<Sample> v);
  Coord net_force(Coord c, vector<Sample> v);
  Coord net_force1(Coord c, vector<Sample> v);

  virtual void algorithm(Sample) = 0; // override this
};

class Vivaldi1 : public Vivaldi {
 public:
  Vivaldi1(Node *n, int d) : Vivaldi(n,d) { }
  void algorithm(Sample);
};

class Vivaldi2 : public Vivaldi {
 public:
  double _damp;
  Vivaldi2(Node *n, int d) : Vivaldi(n,d), _damp(0.1) { }
  void algorithm(Sample);
};

class Vivaldi3 : public Vivaldi {
 public:
  double _jumpprob;
  Vivaldi3(Node *n, int d) : Vivaldi(n,d), _jumpprob(0.1) { }
  void algorithm(Sample);
};

class Vivaldi4 : public Vivaldi3 {
 public:
  Vivaldi4(Node *n, int d) : Vivaldi3(n,d) { }
  void algorithm(Sample);
};

class Vivaldi5 : public Vivaldi3 {
 public:
  Vivaldi5(Node *n, int d) : Vivaldi3(n,d) { }
  void algorithm(Sample);
};

class Vivaldi6 : public Vivaldi3 {
 public:
  Vivaldi6(Node *n, int d) : Vivaldi3(n,d) { }
  void algorithm(Sample);
};

class Vivaldi7 : public Vivaldi {
 public:
  double _damp;
  double _jumpprob;
  Vivaldi7(Node *n, int d) : Vivaldi(n,d), _damp(0.1), _jumpprob(0.1) { }
  void algorithm(Sample);
};

class Vivaldi8 : public Vivaldi3 {
 public:
  Vivaldi8(Node *n, int d) : Vivaldi3(n,d) { }
  void algorithm(Sample);
};

class Vivaldi9 : public Vivaldi3 {
 public:
  Vivaldi9(Node *n, int d) : Vivaldi3(n,d) { }
  void algorithm(Sample);
};

class Vivaldi10 : public Vivaldi {
 public:
  Vivaldi10(Node *n, int d) : Vivaldi (n,d) { }
  void algorithm(Sample);
};

inline double
dist(Vivaldi::Coord a, Vivaldi::Coord b)
{
  double d = 0.0;
  assert (a._v.size () == b._v.size ());
  for (unsigned int i = 0; i < a._v.size (); i++) 
    d += (a._v[i] - b._v[i])*(a._v[i] - b._v[i]);

  return sqrt(d);
}

inline Vivaldi::Coord
operator-(Vivaldi::Coord a, Vivaldi::Coord b)
{
  Vivaldi::Coord c;
  assert (a._v.size () == b._v.size ());
  for (unsigned int i = 0; i < a._v.size (); i++) 
    c._v.push_back (a._v[i] - b._v[i]);
  return c;
}

inline Vivaldi::Coord
operator+(Vivaldi::Coord a, Vivaldi::Coord b)
{
  Vivaldi::Coord c;
  assert (a._v.size () == b._v.size ());
  for (unsigned int i = 0; i < a._v.size (); i++) 
    c._v.push_back(a._v[i] + b._v[i]);

  return c;
}

inline Vivaldi::Coord
operator/(Vivaldi::Coord c, double x)
{
  for (unsigned int i = 0; i < c._v.size (); i++) 
    c._v[i] /= x;
  return c;
}

inline Vivaldi::Coord
operator*(Vivaldi::Coord c, double x)
{
  for (unsigned int i = 0; i < c._v.size (); i++) 
    c._v[i] *= x;
  return c;
}

inline double
length(Vivaldi::Coord c)
{
  double l = 0.0;
  for (unsigned int i = 0; i < c._v.size (); i++) 
    l += c._v[i]*c._v[i];
  return sqrt(l);
}

template<class BT, class AT, class RT>
bool Vivaldi::doRPC(IPAddress dst, BT *target, void (BT::*fn)(AT*, RT*),
                    AT *args, RT *ret) {
  // target is probably the result of a dynamic_cast<BT*>...
  assert(target);

  class Thunk {
  public:
    BT *_target;
    void (BT::*_fn)(AT *, RT *);
    AT *_args;
    RT *_ret;
    Vivaldi *_vtarget;
    Coord _c;
    static void thunk(void *xa) {
      Thunk *t = (Thunk *) xa;
      (t->_target->*(t->_fn))(t->_args, t->_ret);
      t->_c = t->_vtarget->my_location();
    }
  };

  Thunk *t = new Thunk;
  t->_target = target;
  t->_fn = fn;
  t->_args = args;
  t->_ret = ret;
  t->_vtarget = find(dst);
  
  Time before = now();
  bool ok = _n->_doRPC(dst, Thunk::thunk, (void *) t);
  if(ok)
    sample(dst, t->_c, now() - before);

  delete t;
  return ok;
}

#if 0
// Make an RPC call, but time it and tell Vivaldi.
// Basically wraps the RPC in an RPC to rpc_handler.
// Use this only for simple RPCs: don't use it for
// recursive RPCs.
template<class BT, class AT, class RT>
bool Vivaldi::RPC(IPAddress dsta,
                  void (BT::* fn)(AT *, RT *),
                  AT *args,
                  RT *ret)
{
  Vivaldi *vtarget = find(dsta);
  assert(vtarget);

  // find target node from IP address.
  Node *dstnode = Network::Instance()->getnode(dsta);
  assert(dstnode && dstnode->ip() == dsta);

  // find target protocol from class name.
  Protocol *dstproto = dstnode->getproto(typeid(BT));
  BT *target = dynamic_cast<BT*>(dstproto);
  assert(target);

  struct rpc_glop {
    BT *_target;
    Vivaldi *_vtarget;
    void (BT::* _fn)(AT *, RT *);
    AT *_args;
    RT *_ret;
    Coord _c;
    static void thunk(void *xg){
      rpc_glop *g = (rpc_glop*) xg;
      (g->_target->*(g->_fn))(g->_args, g->_ret);
      g->_c = g->_vtarget->my_location();
    }
  };

  rpc_glop *gp = new rpc_glop;
  gp->_target = target;
  gp->_vtarget = vtarget;
  gp->_fn = fn;
  gp->_args = args;
  gp->_ret = ret;

  Time before = now();

  bool ok = Node::_doRPC(dsta, rpc_glop::thunk, (void*) gp);

  if(ok){
    Time after = now();
    sample(dsta, gp->_c, after - before);
  }

  delete gp;

  return ok;
}
#endif

#endif


