class str;
#include <vec.h>

// Perhaps this should be made a coordinate in the sort of standard
// object-oriented sense of the word, instead of just a namespace
// for some common functions.
struct Coord {
  static void print_vector (str a, const vec<float> &v);
  static float distance_f (const vec<float> &a, const vec<float> &b);
  static vec<float> vector_add (const vec<float> &a, const vec<float> &b);
  static vec<float> vector_sub (const vec<float> &a, const vec<float> &b);
  static float norm (const vec<float> &a);
  static vec<float> scalar_mult (const vec<float> &v, float s);
};
