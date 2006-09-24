#include "coord.h"
#include <str.h>
#include <err.h>
#include <math.h>

#define SET_COORDS(NC, val)               \
   coords.clear ();                         \
   for (unsigned int i = 0; i < NC; i++) { \
      coords.push_back (val);               \
   }                                       \

Coord::Coord () 
{
  SET_COORDS (NCOORD, 0.0);
  ht = 1.0;
  pred_err = -1;
};

Coord::Coord (const chord_node &n)
{
  SET_COORDS (NCOORD, n.coords[i]);
  //last coord is height if using it
  if (USING_HT) ht = n.coords[NCOORD]; 
  else
    ht = 1.0;
  pred_err = n.e;
}

Coord::Coord (const chord_node_wire &n)
{
  SET_COORDS (NCOORD, n.coords[i]);
  if (USING_HT) ht = n.coords[NCOORD]; 
  pred_err = n.e;
}

void 
Coord::set (const Coord &c)
{
  SET_COORDS (c.size (), c.coords[i]);
  if (USING_HT) ht = c.ht; 
  pred_err = c.pred_err;
}

void
Coord::set (const chord_node &n)
{
  SET_COORDS (NCOORD, n.coords[i]);
  if (USING_HT) ht = n.coords[NCOORD]; 
  pred_err = n.e;
}

void 
Coord::fill_node (chord_node &data) const 
{
  data.coords.setsize (NCOORD + USING_HT);
  for (unsigned int i = 0; i < NCOORD; i++)
    data.coords[i] = static_cast<int> (coords[i]);
  if (USING_HT) data.coords[NCOORD] = ( static_cast<int> (ht));
  data.e = pred_err;
}

void 
Coord::fill_node (chord_node_wire &data) const 
{
  for (unsigned int i = 0; i < NCOORD; i++)
    data.coords[i] = static_cast<int> (coords[i]);
  if (USING_HT) data.coords[NCOORD] = static_cast<int> (ht);
  data.e = pred_err;
}

void
Coord::print (str a)
{
  warn << a << ": ";
  for (unsigned int i = 0; i < coords.size (); i++)
    warnx << (int)(coords[i]) << " ";
  if (USING_HT) warnx << "ht= " << (int)ht << " ";
  warnx << " with scaled error: " << pred_err << "\n";
  warnx << "\n";
}



float 
Coord::distance_f (const Coord &c) 
{
  float f = 0.0;
  for (unsigned int i = 0; i < coords.size (); i++)
    f += (c.coords[i] - coords[i])*(c.coords[i] - coords[i]);
  f = sqrt (f);
  if (USING_HT)
    f += ht + c.ht;
  return f;
}

float 
Coord::distance_f (const chord_node_wire &c) 
{
  float f = 0.0;
  for (unsigned int i = 0; i < NCOORD; i++)
    f += (c.coords[i] - coords[i])*(c.coords[i] - coords[i]);
  f = sqrt (f);
  
  //ht is encoded as last coord in chord_node_wire
  if (USING_HT)
    f += ht + c.coords[NCOORD];
  return f;
}

float
Coord::distance_f (const Coord &a, const Coord &b)
{

  float f = 0.0;
  for (unsigned int i = 0; i < a.size (); i++)
    f += (a.coords[i] - b.coords[i])*(a.coords[i] - b.coords[i]);
  f = sqrt (f);
  if (USING_HT)
    f += a.ht + b.ht;
  return f;
}

void
Coord::vector_add (const Coord &b)
{
  for (unsigned int i = 0; i < coords.size (); i++)
    coords[i] = (coords[i] +  b.coords[i]);
  ht += b.ht;
}

void
Coord::vector_sub (const Coord &b)
{
  for (unsigned int i = 0; i < coords.size (); i++)
    coords[i] = (coords[i] - b.coords[i]);
  ht += b.ht;
}

float
Coord::norm ()
{
  float ret = 0.0;
  for (unsigned int i = 0; i < coords.size (); i++)
    ret += coords[i]*coords[i];
  if (USING_HT)
    ret += ht;
  return ret;
}

float
Coord::plane_norm ()
{
  float ret = 0.0;
  for (unsigned int i = 0; i < coords.size (); i++)
    ret += coords[i]*coords[i];
  return ret;
}


void
Coord::scalar_mult (float s)
{
  for (unsigned int i = 0; i < coords.size (); i++)
    coords[i] = (coords[i]*s);
  ht *= s;
}

