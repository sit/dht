#include "vivaldi.h"
#include "euclidean.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

Vivaldi::Vivaldi(Node *n, int d)
{
  _n = n;
  _nsamples = 0;
  _dim = d;

  // Start out at a random point.
  // Units are the same as Euclidean::Coords, presumably
  // milliseconds.
#if 0
  Euclidean *t =
    dynamic_cast<Euclidean*>(Network::Instance()->gettopology());
  assert(t);
  Euclidean::Coord rc = t->getcoords(n->ip());
  _c._x = rc.first + (random() % 10);
  _c._y = rc.second + (random() % 10);
#else

  for (int i = 0; i < _dim; i++) 
    _c._v.push_back(random() % 50000 - 25000);

#endif
}

Vivaldi::~Vivaldi()
{
}


ostream& 
operator<< (ostream &s, Vivaldi::Coord &c)
{
  return s<< "(" << c._v[0] << ", " << c._v[1] << ")";
}

ostream& 
operator<< (ostream &s, Vivaldi::Sample &c)
{
  return s<< c._c << " @ " << c._latency << "usecs" ;
}
// latency should be one-way, i.e. RTT / 2
void
Vivaldi::sample(IPAddress who, Coord c, double latency)
{
  algorithm(Sample(c, latency));
  _nsamples += 1;
}

Vivaldi::Coord
Vivaldi::net_force1(Coord c, vector<Sample> v)
{
  Coord f(v[0]._c.dim());

  for(unsigned i = 0; i < v.size(); i++){
    Sample s = v[i];
    double d = dist(c, s._c);
    if(d > 0.01){
      Coord direction = (s._c - c);
      direction = direction / d;
      f = f + (direction * (d - s._latency));
    }
  }
  return f;
}

Vivaldi::Coord
Vivaldi::net_force(Coord c, vector<Sample> v)
{
  Coord f(v[0]._c.dim());

  for(unsigned i = 0; i < v.size(); i++){
    //    double noise = (double)(random () % (int)v[i]._latency) / 10.0;
    double noise = 0;
    double actual = v[i]._latency + noise;
    double expect = dist (c, v[i]._c);
    if(actual >= 0 && (expect > 0.01) ){
      double grad = expect - actual;
      Coord dir = (v[i]._c - c);
      double unit = 1.0/length(dir);
      Vivaldi::Coord udir = dir * unit * grad;
      f = f + udir;
    }
  }
  return f;
}

// return the Sample whose distance is most wrong.
// absolute seems to work better than relative.
Vivaldi::Sample
Vivaldi::wrongest(vector<Sample> v)
{
  unsigned i, xx = 0;
  double max = -1;
  for(i = 0; i < v.size(); i++){
    double d = dist(_c, v[i]._c);
    double dd = d - v[i]._latency;
    if(dd < 0)
      dd = -dd;
    if(dd > max){
      max = dd;
      xx = i;
    }
  }
  return v[xx];
}

Vivaldi::Sample
Vivaldi::lowest_latency(vector<Sample> v)
{
  unsigned i, xx = 0;
  double min = 1000000000;
  for(i = 0; i < v.size(); i++){
    if(v[i]._latency < min){
      min = v[i]._latency;
      xx = i;
    }
  }
  return v[xx];
}

// Figure 1 from SOSP 2003 submission.
void
Vivaldi1::algorithm(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  Coord f = net_force(_c, _samples);

  // apply the force to our coordinates
  _c = _c + (f * 0.001);

  _samples.clear();
}

// the current implementation
void
Vivaldi10::algorithm(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  Coord f = net_force(_c, _samples);
  //  Coord f1 = net_force1(_c, _samples);
  
  //  cerr << f << " " << f1 << "\n";
  double t = 0.01;
  float ftot = 0;
  for (int i = 0; i < f.dim(); i++)
    ftot += f._v[i];

  while (ftot*t > 100.0) t /= 2.0;

  // apply the force to our coordinates
  _c = _c + (f * t);

  if (_samples.size () > 16)
    _samples.erase(_samples.begin()); //this is so much better than pop_front
}

// algo1(), but starts without much damping, and gradually
// damps more and more.
void
Vivaldi2::algorithm(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  Coord f = net_force(_c, _samples);

  // apply the force to our coordinates
  _c = _c + (f * _damp);

  _damp *= 0.95;
  if(_damp < 0.001)
    _damp = 0.001;

  _samples.clear();
}

// algo1(), but occasionally jumps to correct its distance
// to the sample which the most absolute error, with
// exponentially decreasing frequency.
// This works pretty badly because it often jumps very far
// away in a random direction to correct for a sample
// that it's too close to.
void
Vivaldi3::algorithm(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  if(randf() < _jumpprob){
    vector<Sample> v;
    v.push_back(wrongest(_samples));
    Coord f = net_force(_c, v);
    _c = _c + f;
    _jumpprob /= 1.1;
  } else {
    Coord f = net_force(_c, _samples);

    // apply the force to our coordinates
    _c = _c + (f * 0.001);
  }

  _samples.clear();
}

// Like algo1(), but occasionally jump to correct the distance
// to the sample with the lowest latency.
void
Vivaldi4::algorithm(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  if(randf() < _jumpprob){
    vector<Sample> v;
    v.push_back(lowest_latency(_samples));
    Coord f = net_force(_c, v);
    _c = _c + f;
    _jumpprob /= 1.1;
  } else {
    Coord f = net_force(_c, _samples);
    _c = _c + (f * 0.001);
  }

  _samples.clear();
}

// Like algo4(), but occasionally jump to the location of
// the sample with the lowest latency.
// Sometimes we will jump to our own location, but it probably
// doesn't really matter.
void
Vivaldi5::algorithm(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  if(randf() < _jumpprob){
    Sample s = lowest_latency(_samples);
    _c = s._c;
    _jumpprob /= 1.1;
  } else {
    Coord f = net_force(_c, _samples);
    _c = _c + (f * 0.001);
  }

  _samples.clear();
}

// Occasionally jump to the location of a randomly chosen sample node.
// Works worse than any of algorithms 1 - 5.
void
Vivaldi6::algorithm(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  if(randf() < _jumpprob){
    _c = _samples[random() % _samples.size()]._c;
    _jumpprob /= 1.1;
  } else {
    Coord f = net_force(_c, _samples);
    _c = _c + (f * 0.001);
  }

  _samples.clear();
}

// Combination of 2 and 5.
// This one really sucks. It starts converging very quickly, then
// after about 100 samples it actually starts to diverge, and doesn't
// start converging again until about 10000 samples. I think the
// whole random-jump plan is a mistake.
void
Vivaldi7::algorithm(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  if(randf() < _jumpprob){
    Sample s = lowest_latency(_samples);
    _c = s._c;
    _jumpprob /= 1.1;
  } else {
    Coord f = net_force(_c, _samples);
    _c = _c + (f * _damp);
    _damp *= 0.95;
    if(_damp < 0.001)
      _damp = 0.001;
  }

  _samples.clear();
}

// Like 6, but only jump if the new position
// has lower apparent error.
// Starts strong but never gets even as good as algorithm 1.
void
Vivaldi8::algorithm(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  Coord f = net_force(_c, _samples);

  if(randf() < _jumpprob){
    Sample s = _samples[random() % _samples.size()];
    Coord f1 = net_force(s._c, _samples);
    if(length(f1) < length(f) * 0.97){
      _c = s._c;
    }
    _jumpprob /= 1.1;
  } else {
    _c = _c + (f * 0.001);
  }

  _samples.clear();
}

// Like 1, but every once in a while jump all the way to
// "best" position.
// Starts off a bit slower than 2, but eventually overtakes it. Slightly.
void
Vivaldi9::algorithm(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  Coord f = net_force(_c, _samples);

  if(randf() < _jumpprob){
    _c = _c + (f * 0.1);
    _jumpprob /= 1.1;
  } else {
    _c = _c + (f * 0.001);
  }

  _samples.clear();
}

// variants:
// somehow have a few nodes choose themselves as landmarks,
//   they agree on positions, everyone else follows.
// add nodes one at a time
// more dimensions
// every sample by itself, not every 10

// hmm, it seems like the system does *exactly* as well if each
// node only talks to a few other nodes as it does when all nodes
// cycle randomly among the other nodes. maybe that's because the
// "few" nodes i talk to are the next higher ones in IP address
// space, so information flows pretty regularly, just a bit slower.
// perhaps also because everybody is joining at the same time, so
// at least early on there is not much point in talking to lots
// of other nodes.
// OK, this turns out probably not to be true. I think early on
// it's relatively easy to fix errors with local movement,
// so things look good with just a few neighbors. Easy to see
// this with Euclidean cross layout.

// general lesson from occasional jumps: they don't work.

// spring relaxation doesn't seem to work any better than the
// much stupider scheme of moving to eliminate 1/100th of the
// error in the latency to each sample. I.e. not saving samples
// at all.

// maybe need periodic "jolt" as in simulated annealing.

// when the real coordinates are all in a straight line, vivaldi
// has trouble positioning some of the nodes, they end up scattered
// around the two ends of the line. perhaps because they cannot
// squeeze through. maybe a 3rd dimension would help. and starting
// from uniform random (not on a line) is no better.

// it never seems to get particularly accurate results, even in
// this easy case in which the nodes really are 2d and there
// are no errors. that is, the average error never falls below
// about 25.

// OK! maybe starting all the nodes at the same time w/ random
// positions is too challenging. Try adding them one by one.

// Maybe a node should start with a large damping constant
// (i.e. the 0.001) so it quickly finds an initial position,
// and then decrease it. maybe this is similar to gradually
// decreasing probability of a random jump?

// with _damp, it diverges if it starts with >=0.5, but is fine
// if it starts as 0.1. and 0.1 converges pretty quick, much
// faster than w/o decreasing damping.
