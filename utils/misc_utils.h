#ifndef _MISC_UTILS_H_
#define _MISC_UTILS_H_

#include <chord_types.h>
#include <str.h>
#include <vec.h>

bool in_vector (const vec<chordID> &v, chordID N);

str gettime ();
u_int64_t getusec ();

u_int32_t uniform_random(double a, double b);
float uniform_random_f (float max);

chord_hostname my_addr ();

inline const strbuf &
strbuf_cat (const strbuf &sb, const net_address &r)
{
  sb << r.hostname << ":" << r.port;
  return sb;
}

inline const strbuf &
strbuf_cat (const strbuf &sb, const chord_node &n)
{
  sb << n.x << "@" << n.r;
  return sb;
}

#endif /* _MISC_UTILS_H_ */
