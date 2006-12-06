#ifndef __COORD_H__
#define __COORD_H__

class str;
#include <vec.h>
#include <chord_types.h>

// Perhaps this should be made a coordinate in the sort of standard
// object-oriented sense of the word, instead of just a namespace
// for some common functions.
struct Coord {
  static const unsigned int NCOORD = 2;
  static const unsigned int USING_HT = 1;
  static const float PRED_ERR_MULT = 1000.0;

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
  Coord (const Coord &c) : coords (c.coords), ht (c.ht), pred_err (c.pred_err) {};
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

#endif /* __CHORD_H__ */
