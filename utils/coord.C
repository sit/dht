#include "coord.h"
#include <str.h>
#include <err.h>
#include <math.h>

void
Coord::print_vector (str a, const vec<float> &v)
{
  warn << a << ": ";
  for (unsigned int i = 0; i < v.size (); i++)
    warnx << (int)(v[i]*1000.0) << " ";
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

vec<float>
Coord::vector_add (const vec<float> &a, const vec<float> &b)
{
  vec<float> ret;
  for (unsigned int i = 0; i < a.size (); i++)
    ret.push_back (a[i] +  b[i]);
  return ret;
}

vec<float>
Coord::vector_sub (const vec<float> &a, const vec<float> &b)
{
  vec<float> ret;
  for (unsigned int i = 0; i < a.size (); i++)
    ret.push_back (a[i] - b[i]);
  return ret;
}

float
Coord::norm (const vec<float> &a)
{
  float ret = 0.0;
  for (unsigned int i = 0; i < a.size (); i++)
    ret += a[i]*a[i];
  return ret;
}

vec<float>
Coord::scalar_mult (const vec<float> &v, float s)
{
  vec<float> ret;
  for (unsigned int i = 0; i < v.size (); i++)
    ret.push_back (v[i]*s);
  return ret;
}
