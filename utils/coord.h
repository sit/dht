class str;
#include <vec.h>
#include <chord_types.h>

// Perhaps this should be made a coordinate in the sort of standard
// object-oriented sense of the word, instead of just a namespace
// for some common functions.
struct Coord {
  static void print_vector (str a, const vec<float> &v);
  static float distance_f (const vec<float> &a, const vec<float> &b);
  static float distance_f (const vec<float> &a, const chord_node &n);

  static void vector_add (vec<float> &a, const vec<float> &b);
  static void vector_sub (vec<float> &a, const vec<float> &b);
  static float norm (const vec<float> &a);
  static void scalar_mult (vec<float> &v, float s);
};

#if defined (__ppc__)
#define sqrtf sqrt
#endif

