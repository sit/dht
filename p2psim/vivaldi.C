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
  for(unsigned i = 0; i < _samples.size(); i++){
    double d = dist(_c, _samples[i]._c);
    Coord direction = (_samples[i]._c - _c);
    if(length(direction) > 0.01){
      direction = direction / length(direction);
      f = f + (direction * (d - _samples[i]._latency));
    }
  }
  return f;
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

  _damp *= 0.99;
  if(_damp < 0.001)
    _damp = 0.001;

  _samples.clear();
}

// latency should be one-way, i.e. RTT / 2
void
Vivaldi::sample(IPAddress who, Coord c, double latency)
{
  algo1(Sample(c, latency));
  _nsamples += 1;
}

// variants:
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
