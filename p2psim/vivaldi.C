#include "vivaldi.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

Vivaldi::Vivaldi(IPAddress me)
{
  _me = me;

  // To avoid over or underflowing a 32-bit unsigned,
  // start us out at a random point near the middle.
  // Units are the same as Euclidean::Coords, presumably
  // milliseconds.
  _x = 10000 + (random() % 200);
  _y = 10000 + (random() % 200);
}

Vivaldi::~Vivaldi()
{
}

void
Vivaldi::sample(IPAddress who, Coord c, unsigned latency)
{
  // compute the distance implied by the coordinates.
  double dx = c.first - _x;
  double dy = c.second - _y;
  double d = sqrt(dx*dx + dy*dy);

  if(d > 0){
    // move 1/100th of the way towards the position
    // implied by the latency.
    dx = (dx / d) * (d - latency) / 100.0;
    dy = (dy / d) * (d - latency) / 100.0;

    _x += dx;
    _y += dy;
  }
}

Vivaldi::Coord
Vivaldi::my_location()
{
  return Coord((unsigned) _x, (unsigned) _y);
}

void
Vivaldi::rpc_handler(rpc_args *args, rpc_ret *ret)
{
  //  (this->*args->fn)(args->args, args->ret);
  //  ret->c = my_location();
}
