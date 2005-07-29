#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <async.h>

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
  updatetime_ = timenow;
}

location::location (const chordID &n, 
		    const net_address &r, 
		    const int v,
		    const Coord &coords,
		    time_t k,
		    time_t a,
		    int32_t b,
		    bool m) 
  : n_ (n),
    addr_ (r),
    vnode_ (v),
    a_lat_ (0.0),
    a_var_ (0.0),
    alive_ (true),
    dead_time_ (0),
    nrpc_ (0),
    knownup_(k),
    age_(a),
    budget_(b),
    isme_(m)
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
    nrpc_ (0),
    knownup_(node.knownup),
    age_(node.age),
    budget_(node.budget),
    isme_(false)
{
  coords_.set (node);
  init ();
}

location::~location () {
}

void
location::update_knownup () 
{
  knownup_ = (timenow - updatetime_); 
  assert(isme_);
}

void
location::update_age ()
{
  if (!isme_) {
    // timenow is updated by libasync core.C
    age_ += (timenow - updatetime_); 
    updatetime_ = timenow;
  }
}

void
location::update (ptr<location> l)
{
  update_age ();
  if (l->age () < (age_ + 10)) {
    age_ = l->age ();
    knownup_ = l->knownup ();
    budget_ = l-> budget ();
    coords_.set (l->coords ());
  }
}

void
location::fill_node (chord_node &data)
{
  data.x = n_;
  data.r = addr_;
  data.vnode_num = vnode_;

  //for Accordion
  this->update_age ();
  data.knownup = knownup_;
  data.age = age_;
  data.budget = budget_;

  coords_.fill_node (data);
}

void
location::fill_node (chord_node_wire &data) 
{
  /* saddr fields are in network byte order */
  data.machine_order_ipv4_addr = ntohl (saddr_.sin_addr.s_addr);
  data.machine_order_port_vnnum = (ntohs (saddr_.sin_port) << 16) | vnode_;

  //for Accordion
  this->update_age ();
  data.knownup = knownup_;
  data.age = age_;
  data.budget = budget_;
 
  coords_.fill_node (data);
}

void
location::fill_node_ext (chord_node_ext &data)
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
    dead_time_ = timenow;
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


const strbuf &
strbuf_cat (const strbuf &sb, const ref<location> l)
{
  chord_node n;
  l->fill_node (n);
  sb << n;
  return sb;
}
