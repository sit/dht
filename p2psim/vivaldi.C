#include "vivaldi.h"
#include "euclidean.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

map<IPAddress, Vivaldi *> Vivaldi::_nodes;

Vivaldi::Vivaldi(IPAddress me)
{
  _me = me;

  // To avoid over or underflowing a 32-bit unsigned,
  // start us out at a random point near the middle.
  // Units are the same as Euclidean::Coords, presumably
  // milliseconds.
  _c._x = 10000 + (random() % 200);
  _c._y = 10000 + (random() % 200);

  _nodes[me] = this;
}

Vivaldi::~Vivaldi()
{
}

void
Vivaldi::sample(IPAddress who, Coord c, unsigned latency)
{
  // compute the distance implied by the coordinates.
  double dx = c._x - _c._x;
  double dy = c._x - _c._y;
  double d = sqrt(dx*dx + dy*dy);

  if(d > 0){
    // move 1/100th of the way towards the position
    // implied by the latency.
    dx = (dx / d) * (d - latency) / 100.0;
    dy = (dy / d) * (d - latency) / 100.0;

    _c._x += dx;
    _c._y += dy;
  }

#if 0
  Euclidean *t = dynamic_cast<Euclidean*>(Network::Instance()->gettopology());
  assert(t);
  Euclidean::Coord rc = t->getcoords(_me);
  printf("vivaldi %u %u %u %.1f %.1f\n",
         _me,
         rc.first,
         rc.second,
         _c._x,
         _c._y);
#endif
}
