#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <amisc.h>

#include "location.h"
#include "misc_utils.h"
#if 0
#include <chord_util.h>
#endif /* 0 */
#include "modlogger.h"


#define trace modlogger ("location")

void
location::init ()
{
  bzero(&saddr_, sizeof(sockaddr_in));
  saddr_.sin_family = AF_INET;
  inet_aton (addr_.hostname.cstr (), &saddr_.sin_addr);
  saddr_.sin_port = htons (addr_.port);
#if 0
  if (!is_authenticID (n_, addr_.hostname, addr_.port, vnode_)) {
    trace << "badnode " << n_ << " " << addr_ << " " << vnode_ << "\n";
    vnode_ = -1;
  }
#endif /* 0 */  
}

location::location (const chordID &n, 
		    const net_address &r, 
		    const int v,
		    const vec<float> &coords) 
  : n_ (n),
    addr_ (r),
    vnode_ (v),
    a_lat_ (0.0),
    a_var_ (0.0),
    alive_ (true),
    dead_time_ (0),
    nrpc_ (0)
{
  coords_ = coords;
  init ();
}

location::location (const chord_node &node) 
  : n_ (node.x),
    addr_ (node.r),
    vnode_ (node.vnode_num),
    a_lat_ (0.0),
    a_var_ (0.0),
    alive_ (true),
    dead_time_ (0),
    nrpc_ (0)
{
  for (unsigned int i = 0; i < node.coords.size (); i++)
    coords_.push_back (node.coords[i]);
  init ();
}

location::~location () {
}

void
location::fill_node (chord_node &data) const
{
  data.x = n_;
  data.r = addr_;
  data.vnode_num = vnode_;
  data.coords.setsize (coords_.size ());
  for (unsigned int i = 0; i < coords_.size (); i++)
    data.coords[i] = static_cast<int> (coords_[i]);
}

void
location::fill_node (chord_node_wire &data) const
{
  data.r = addr_;
  data.vnode_num = vnode_;
  data.coords.setsize (coords_.size ());
  for (unsigned int i = 0; i < coords_.size (); i++)
    data.coords[i] = static_cast<int> (coords_[i]);
}

void
location::fill_node_ext (chord_node_ext &data) const
{
  fill_node (data.n);
  data.a_lat = static_cast<int> (a_lat_);
  data.a_var = static_cast<int> (a_var_);
  data.nrpc  = nrpc_;
}

void
location::set_alive (bool alive)
{
  if (!alive) {
    timespec ts;
    clock_gettime (CLOCK_REALTIME, &ts);
    dead_time_ = ts.tv_sec;
  }
  alive_ = alive;
}

void
location::set_coords (const vec<float> &coords)
{
  coords_.clear ();
  for (unsigned int i = 0; i < coords.size (); i++)
    coords_.push_back(coords[i]);
}
