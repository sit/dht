#ifndef __VIVALDI_H
#define __VIVALDI_H

#include "network.h"

// Compute Vivaldi synthetic coordinates.
// Protocol-independent: doesn't care where the measurements
// come from.
// Indexed by IPAddress.
// Anyone can create a Vivaldi, call sample() after each
// RPC, and then call my_location().
// Or you can use Vivaldi::doRPC.

class Vivaldi : Protocol {
 public:
  Vivaldi(Node *n, int d);
  virtual ~Vivaldi();
  string proto_name() { return "Vivaldi"; }

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

  struct Sample {
    Coord _c;
    double _latency;
    IPAddress _who;
    Sample(Coord c, double l, IPAddress w) { _c = c; _latency = l; _who = w;}
  };

 protected:
  int _nsamples; // how many times sample() has been called
  int _dim; //dimensionality of the fit space
  Coord _c; // current estimated coordinates
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
  double _timestep;
  double _bbox_max;
  double _curts;
  int _adapt;
 public:
  Vivaldi10(Node *n, int d, double t, int ad) : Vivaldi (n,d), 
    _timestep (t), _bbox_max (100000), _curts(1.0), _adapt (ad) { };
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
  assert(dst);

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

  Thunk *t = New Thunk;
  t->_target = target;
  t->_fn = fn;
  t->_args = args;
  t->_ret = ret;
  t->_vtarget = dynamic_cast<Vivaldi*>(getpeer(dst));
  assert(t->_vtarget);
  
  Time before = now();
  bool ok = node()->_doRPC(dst, Thunk::thunk, (void *) t);
  if(ok)
    sample(dst, t->_c, (now() - before) / 2.0);

  delete t;
  return ok;
}

ostream& operator<< (ostream &s, Vivaldi::Coord &c);

#endif
