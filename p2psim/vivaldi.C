#include "vivaldi.h"
#include "euclidean.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

Vivaldi::Vivaldi(Node *n)
{
  _n = n;

  // To avoid over or underflowing a 32-bit unsigned,
  // start us out at a random point near the middle.
  // Units are the same as Euclidean::Coords, presumably
  // milliseconds.
  _c._x = 10000 + (random() % 200);
  _c._y = 10000 + (random() % 200);
  _nsamples = 0;
}

Vivaldi::~Vivaldi()
{
}

void
Vivaldi::sample(IPAddress who, Coord c, unsigned latency)
{
  // compute the distance implied by the coordinates.
  double dx = c._x - _c._x;
  double dy = c._y - _c._y;
  double d = sqrt(dx*dx + dy*dy);

  if(d > 0.01){
    // move 1/100th of the way towards the position
    // implied by the latency.
    dx = (dx / d) * (d - latency) / 100.0;
    dy = (dy / d) * (d - latency) / 100.0;

    _c._x += dx;
    _c._y += dy;
    _nsamples += 1;
  }

  Euclidean *t = dynamic_cast<Euclidean*>(Network::Instance()->gettopology());
  assert(t);
  Euclidean::Coord rc = t->getcoords(_n->ip());
  printf("vivaldi %u %d %u %u %.1f %.1f\n",
         _n->ip(),
         _nsamples,
         rc.first,
         rc.second,
         _c._x,
         _c._y);
}
