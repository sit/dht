#ifndef __VIVALDI_H
#define __VIVALDI_H

#include <vector>
#include "p2psim.h"
#include "protocol.h"
#include "network.h"
#include "protocolfactory.h"
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
  Vivaldi(Node *n);
  virtual ~Vivaldi();
  int nsamples() { return _nsamples; }

  struct Coord {
    double _x;
    double _y;
  };

  void sample(IPAddress who, Coord c, double latency);
  Coord my_location() { return _c; }

  // Anyone can use this to make an RPC and have Vivaldi time it.
  template<class BT, class AT, class RT>
    bool RPC(IPAddress a, void (BT::* fn)(AT *, RT *),
             AT *args, RT *ret);

 protected:
  Node *_n; // this node
  int _nsamples; // how many times sample() has been called
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

  virtual void algorithm(Sample) = 0; // override this
};

class Vivaldi1 : public Vivaldi {
 public:
  Vivaldi1(Node *n) : Vivaldi(n) { }
  void algorithm(Sample);
};

class Vivaldi2 : public Vivaldi {
 public:
  double _damp;
  Vivaldi2(Node *n) : Vivaldi(n), _damp(0.1) { }
  void algorithm(Sample);
};

class Vivaldi3 : public Vivaldi {
 public:
  double _jumpprob;
  Vivaldi3(Node *n) : Vivaldi(n), _jumpprob(0.1) { }
  void algorithm(Sample);
};

class Vivaldi4 : public Vivaldi3 {
 public:
  Vivaldi4(Node *n) : Vivaldi3(n) { }
  void algorithm(Sample);
};

class Vivaldi5 : public Vivaldi3 {
 public:
  Vivaldi5(Node *n) : Vivaldi3(n) { }
  void algorithm(Sample);
};

class Vivaldi6 : public Vivaldi3 {
 public:
  Vivaldi6(Node *n) : Vivaldi3(n) { }
  void algorithm(Sample);
};

class Vivaldi7 : public Vivaldi {
 public:
  double _damp;
  double _jumpprob;
  Vivaldi7(Node *n) : Vivaldi(n), _damp(0.1), _jumpprob(0.1) { }
  void algorithm(Sample);
};

class Vivaldi8 : public Vivaldi3 {
 public:
  Vivaldi8(Node *n) : Vivaldi3(n) { }
  void algorithm(Sample);
};

class Vivaldi9 : public Vivaldi3 {
 public:
  Vivaldi9(Node *n) : Vivaldi3(n) { }
  void algorithm(Sample);
};

inline double
dist(Vivaldi::Coord a, Vivaldi::Coord b)
{
  double dx = a._x - b._x;
  double dy = a._y - b._y;
  return sqrt(dx*dx + dy*dy);
}

inline Vivaldi::Coord
operator-(Vivaldi::Coord a, Vivaldi::Coord b)
{
  Vivaldi::Coord c;
  c._x = a._x - b._x;
  c._y = a._y - b._y;
  return c;
}

inline Vivaldi::Coord
operator+(Vivaldi::Coord a, Vivaldi::Coord b)
{
  Vivaldi::Coord c;
  c._x = a._x + b._x;
  c._y = a._y + b._y;
  return c;
}

inline Vivaldi::Coord
operator/(Vivaldi::Coord c, double x)
{
  c._x /= x;
  c._y /= x;
  return c;
}

inline Vivaldi::Coord
operator*(Vivaldi::Coord c, double x)
{
  c._x *= x;
  c._y *= x;
  return c;
}

inline double
length(Vivaldi::Coord c)
{
  return sqrt(c._x*c._x + c._y*c._y);
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
