#include "coord.h"
#include <str.h>
#include <err.h>
#include <math.h>

void
Coord::print_vector (str a, const vec<float> &v)
{
  warn << a << ": ";
  for (unsigned int i = 0; i < v.size (); i++)
    warnx << (int)(v[i]) << " ";
  warnx << "\n";
}



float
Coord::distance_f (const vec<float> &a, const vec<float> &b)
{
  float f = 0.0;
  for (unsigned int i = 0; i < a.size (); i++)
    f += (a[i] - b[i])*(a[i] - b[i]);

  return sqrtf (f);
}

float
Coord::distance_f (const vec<float> &a, const chord_node &n)
{

  vec<float> b;
  for (u_int i = 0; i < n.coords.size (); i++)
    b.push_back (n.coords[i]);

  float f = 0.0;
  for (unsigned int i = 0; i < a.size (); i++)
    f += (a[i] - b[i])*(a[i] - b[i]);

  return sqrtf (f);
}

void
Coord::vector_add (vec<float> &a, const vec<float> &b)
{
  for (unsigned int i = 0; i < a.size (); i++)
    a[i] = (a[i] +  b[i]);
}

void
Coord::vector_sub (vec<float> &a, const vec<float> &b)
{
  for (unsigned int i = 0; i < a.size (); i++)
    a[i] = (a[i] - b[i]);
}

float
Coord::norm (const vec<float> &a)
{
  float ret = 0.0;
  for (unsigned int i = 0; i < a.size (); i++)
    ret += a[i]*a[i];
  return ret;
}

void
Coord::scalar_mult (vec<float> &v, float s)
{
  for (unsigned int i = 0; i < v.size (); i++)
    v[i] = (v[i]*s);
}
