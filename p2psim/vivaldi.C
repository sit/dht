#include "vivaldi.h"
#include "euclidean.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

Vivaldi::Vivaldi(Node *n)
{
  _n = n;
  _nsamples = 0;

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
  _c._x = (random() % 200) - 100;
  _c._y = (random() % 200) - 100;
#endif

  // algo2()
  _damp = 0.1;

  // algo3()
  _jumpprob = 1.0;
}

Vivaldi::~Vivaldi()
{
}

Vivaldi::Coord
Vivaldi::net_force(vector<Sample> v)
{
  Coord f;
  f._x = 0;
  f._y = 0;
  for(unsigned i = 0; i < v.size(); i++){
    double d = dist(_c, v[i]._c);
    if(d > 0.01){
      Coord direction = (v[i]._c - _c);
      direction = direction / d;
      f = f + (direction * (d - v[i]._latency));
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
Vivaldi::algo1(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  Coord f = net_force(_samples);

  // apply the force to our coordinates
  _c = _c + (f * 0.001);

  _samples.clear();
}

// algo1(), but starts without much damping, and gradually
// damps more and more.
void
Vivaldi::algo2(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  Coord f = net_force(_samples);

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
Vivaldi::algo3(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  if(randf() < _jumpprob){
    vector<Sample> v;
    v.push_back(wrongest(_samples));
    Coord f = net_force(v);
    _c = _c + f;
    _jumpprob /= 2;
  } else {
    Coord f = net_force(_samples);

    // apply the force to our coordinates
    _c = _c + (f * 0.001);
  }

  _samples.clear();
}

// Like algo1(), but occasionally jump to correct the distance
// to the sample with the lowest latency.
void
Vivaldi::algo4(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  if(randf() < _jumpprob){
    vector<Sample> v;
    v.push_back(lowest_latency(_samples));
    Coord f = net_force(v);
    _c = _c + f;
    _jumpprob /= 2;
  } else {
    Coord f = net_force(_samples);
    _c = _c + (f * 0.001);
  }

  _samples.clear();
}

// Like algo4(), but occasionally jump to the location of
// the sample with the lowest latency.
// Sometimes we will jump to our own location, but it probably
// doesn't really matter.
void
Vivaldi::algo5(Sample s)
{
  _samples.push_back(s);
  if(_samples.size() < 10)
    return;

  if(randf() < _jumpprob){
    Sample s = lowest_latency(_samples);
    _c = s._c;
    _jumpprob /= 2;
  } else {
    Coord f = net_force(_samples);
    _c = _c + (f * 0.001);
  }

  _samples.clear();
}

// latency should be one-way, i.e. RTT / 2
void
Vivaldi::sample(IPAddress who, Coord c, double latency)
{
  algo5(Sample(c, latency));
  _nsamples += 1;
}

// variants:
// add nodes one at a time
// force = square of displacement
// more dimensions
// every sample by itself, not every 10
// slowly increase damping
// random jump at exponentially increasing intervals

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
