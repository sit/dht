class str;
#include <vec.h>

extern float gforce;

// Perhaps this should be made a coordinate in the sort of standard
// object-oriented sense of the word, instead of just a namespace
// for some common functions.
struct Coord {
  static void print_vector (str a, const vec<float> &v);
  static float distance_f (const vec<float> &a, const vec<float> &b);
  //  static vec<float> vector_add (const vec<float> &a, const vec<float> &b);
  //  static vec<float> vector_sub (const vec<float> &a, const vec<float> &b);
  static void vector_add (vec<float> &a, const vec<float> &b);
  static void vector_sub (vec<float> &a, const vec<float> &b);
  static float norm (const vec<float> &a);
  static void scalar_mult (vec<float> &v, float s);
  //  static vec<float> scalar_mult (const vec<float> &v, float s);
};
