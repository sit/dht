#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <amisc.h>

#include "location.h"
#include "misc_utils.h"
#include "id_utils.h"
#include "modlogger.h"


#define trace modlogger ("location")

void
location::init ()
{
  bzero(&saddr_, sizeof(sockaddr_in));
  saddr_.sin_family = AF_INET;
  inet_aton (addr_.hostname.cstr (), &saddr_.sin_addr);
  saddr_.sin_port = htons (addr_.port);
  if (!is_authenticID (n_, addr_.hostname, addr_.port, vnode_)) {
    trace << "badnode " << n_ << " " << addr_ << " " << vnode_ << "\n";
    vnode_ = -1;
  }
}

location::location (const chordID &n, 
		    const net_address &r, 
		    const int v,
		    const Coord &coords) 
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
  coords_.set (node);
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
  coords_.fill_node (data);
}

void
location::fill_node (chord_node_wire &data) const
{
  /* saddr fields are in network byte order */
  data.machine_order_ipv4_addr = ntohl (saddr_.sin_addr.s_addr);
  data.machine_order_port_vnnum = (ntohs (saddr_.sin_port) << 16) | vnode_;
  coords_.fill_node (data);
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
  if (!alive && alive_) {
    timespec ts;
    clock_gettime (CLOCK_REALTIME, &ts);
    dead_time_ = ts.tv_sec;
  }
  alive_ = alive;
}

void
location::set_coords (const Coord &coords)
{
  coords_.set (coords);
}

void
location::set_coords (const chord_node &n)
{
  coords_.set (n);
}
