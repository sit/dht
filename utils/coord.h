#ifndef __COORD_H__
#define __COORD_H__

class str;
#include <vec.h>
#include <chord_types.h>

#define NCOORD 2
#define USING_HT 1
#define PRED_ERR_MULT 1000.0

// Perhaps this should be made a coordinate in the sort of standard
// object-oriented sense of the word, instead of just a namespace
// for some common functions.
struct Coord {

  vec<float> coords;
  float ht;


  unsigned int size () const { return coords.size (); };

  void fill_node (chord_node &data) const;
  void fill_node (chord_node_wire &data) const;

  void set (const Coord &coords);
  void set (const chord_node &n);
  Coord ();
  Coord (const chord_node &n);
  Coord (const chord_node_wire &n);
  float err () const { return pred_err/PRED_ERR_MULT; };
  void update_err (float x) {  pred_err = (int)(x*PRED_ERR_MULT); };

  int raw_err () const { return pred_err; };
  void print (str a);

  float distance_f (const Coord &c);
  float distance_f (const chord_node_wire &c);
  float norm ();
  float plane_norm ();
  void scalar_mult (float s);
  void vector_add (const Coord &b);
  void vector_sub (const Coord &b);

  static float distance_f (const Coord &a, const Coord &n);

private:
  int pred_err;

};

#if defined (__ppc__)
#define sqrtf sqrt
#endif

#endif
