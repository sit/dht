#ifndef _LOCATION_H_
#define _LOCATION_H_

#include <vec.h>
#include <chord_types.h>
#include <coord.h>

struct sockaddr_in;
class location {
  const chordID n_;
  const net_address addr_;

  int vnode_;
  Coord coords_;
  float a_lat_;
  float a_var_;
  
  bool alive_;
  time_t dead_time_;

  sockaddr_in saddr_;
  unsigned long nrpc_;

  void init ();
 public:
  location (const chordID &_n, const net_address &_r, const int v, 
	    const Coord &coords);
  location (const chord_node &n);
  ~location ();

  // Accessors;
  const chordID id () const { return n_; }
  const net_address &address () const { return addr_; };
  int vnode () const { return vnode_; }
  const Coord &coords () const { return coords_; };
  Coord coords () { return coords_; };
  float distance () const { return a_lat_; };
  float a_var () const { return a_var_; };
  bool alive () const { return alive_; };
  time_t dead_time () const { return dead_time_; }
  const sockaddr_in &saddr () const { return saddr_; };
  unsigned long nrpc () const { return nrpc_; };

  void fill_node (chord_node &data) const;
  void fill_node (chord_node_wire &data) const;
  void fill_node_ext (chord_node_ext &data) const;

  // Mutators
  void set_alive (bool alive);
  void set_coords (const Coord &coords);
  void set_coords_err (float x) { coords_.update_err (x); };
  void set_coords (const chord_node &n);
  void set_distance (float dist) { a_lat_ = dist; };
  void set_variance (float variance) { a_var_ = variance; };
  void inc_nrpc () { nrpc_++; }
};

const strbuf &strbuf_cat (const strbuf &sb, const ref<location> l);

#endif /* _LOCATION_H */
