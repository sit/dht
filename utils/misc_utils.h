#ifndef _MISC_UTILS_H_
#define _MISC_UTILS_H_

#include <chord_types.h>
#include <str.h>
#include <vec.h>
#include <chord_util.h>

bool in_vector (const vec<chordID> &v, chordID N);

str gettime ();
u_int64_t getusec ();

u_int32_t uniform_random(double a, double b);
float uniform_random_f (float max);

chord_hostname my_addr ();

inline const chord_node
make_chord_node (const chord_node_wire &nl)
{
  chord_node n;
  struct in_addr x;
  x.s_addr = htonl (nl.machine_order_ipv4_addr);
  n.r.hostname = inet_ntoa (x);
  n.r.port     = nl.machine_order_port_vnnum >> 16;
  n.vnode_num  = nl.machine_order_port_vnnum & 0xFFFF;
  n.x = make_chordID (n.r.hostname, n.r.port, n.vnode_num);
  n.coords = nl.coords;
  return n;
}

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

inline const strbuf &
strbuf_cat (const strbuf &sb, const chord_node_wire &n)
{
  sb << make_chord_node (n); // XXX gross
  return sb;
}

#endif /* _MISC_UTILS_H_ */
