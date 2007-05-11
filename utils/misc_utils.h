#ifndef _MISC_UTILS_H_
#define _MISC_UTILS_H_

#include <refcnt.h>
#include <chord_types.h>
#include <str.h>
#include <vec.h>

class location;

bool in_vector (const vec<chordID> &v, chordID N);
bool in_vector (const vec<ptr<location> > &l, chordID N);

str gettime ();
u_int64_t getusec (bool syscall = false);

u_int32_t uniform_random(double a, double b);
float uniform_random_f (float max);

chord_hostname my_addr ();

chord_node make_chord_node (const chord_node_wire &nl);

inline const strbuf &
strbuf_cat (const strbuf &sb, const net_address &r)
{
  sb << r.hostname << ":" << r.port;
  return sb;
}

inline const strbuf &
strbuf_cat (const strbuf &sb, const chord_node &n)
{
  sb << n.x << "," << n.r.hostname << "," << n.r.port << "," << n.vnode_num;
  return sb;
}

inline const strbuf &
strbuf_cat (const strbuf &sb, const chord_node_wire &n)
{
  sb << make_chord_node (n); // XXX gross
  return sb;
}

class chord_trigger_t : public virtual refcount {
  callback<void>::ref cb;
protected:
  chord_trigger_t (callback<void>::ref cb) : cb (cb) {}
public:
  ~chord_trigger_t ();
  static ptr<chord_trigger_t> alloc (callback<void>::ref cb);
};


#endif /* _MISC_UTILS_H_ */
