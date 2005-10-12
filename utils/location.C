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
    knownup_ (k),
    age_ (a),
    budget_ (b),
    isme_ (m),
    losses_ (0)
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
    knownup_ (node.knownup),
    age_ (node.age),
    budget_ (node.budget),
    isme_ (false),
    losses_ (0)
{
  coords_.set (node);
  init ();
}

location::~location () {
}

void
location::update_knownup () 
{
  if ((timenow > 0) && (updatetime_ > 0))
    knownup_ = (timenow - updatetime_); 
  else if ((timenow > 0) && (!updatetime_))
    updatetime_ = timenow;
  if (knownup_ < 30)
    knownup_ = 30;
  assert(isme_ && !age_);
}

time_t
location::age ()
{
  if (!isme_) {
    // timenow is updated by libasync core.C
    if ((timenow > 0) && (updatetime_ > 0)) {
      return (timenow - updatetime_+age_); 
    }else if ((timenow > 0) && (!updatetime_))
      updatetime_ = timenow;
  }
  return age_;
}

void
location::update (ptr<location> l)
{
  // Prefer data that is newer than our own data
  if ((timenow > 0) && (updatetime_ > 0)) {
    if (!isme_ && (l->age ()< ((timenow - updatetime_) + age_))) {
      age_ = l->age ();
      updatetime_ = timenow;
      if (l->knownup ()< knownup_) //assume this is a new start
	losses_ = 0;
      else if (losses_ > 0)
	losses_ --;
      knownup_ = l->knownup ();
      budget_ = l-> budget ();
      coords_.set (l->coords ());
      set_alive (true);
    }
  } else if ((timenow > 0) && (!updatetime_))
    updatetime_ = timenow;
}

void
location::update (chord_node n)
{
  if ((timenow > 0) && (updatetime_ > 0)) {
    if (!isme_ && n.age < ((timenow - updatetime_) + age_)) {
      age_ = n.age;
      updatetime_ = timenow;
      if (n.knownup < knownup_) //assume this is a new start
	losses_ = 0;
      else 
	losses_ = get_loss ();
      knownup_ = n.knownup;
      budget_ = n.budget;
      coords_.set (n);
      set_alive (true);
    }
  } else if ((timenow > 0) && (!updatetime_))
    updatetime_ = timenow;
}

void
location::fill_node (chord_node &data)
{
  data.x = n_;
  data.r = addr_;
  data.vnode_num = vnode_;

  //for Accordion
  data.age = age ();
  if (isme_)
    update_knownup ();
  else if (!data.age)
    data.age = 1; //second-hand routing info has age at least 1
  data.knownup = knownup_;
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
  data.age = age ();
  if (isme_)
    update_knownup ();
  else if (!data.age)
    data.age = 1; //second-hand routing info has age at least 1
  data.knownup = knownup_;
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

char
location::get_loss ()
{
  if (!alive_) return 0;

  if (!dead_time_) 
    dead_time_ = timenow;
  else {
    time_t intval = timenow - dead_time_;
    int tt = intval/5;
    while (tt > 0) {
      losses_ = losses_ >> 1;
      dead_time_ += 5;
      tt--;
    }
  }
  return losses_;
}

void
location::set_loss ()
{
  if (alive_) { 
    losses_ = get_loss () + 1;
    if (losses_ > 4) {
      //too many losses have happened, node is declared dead
      set_alive (false);
      return;
    }
  } 
  updatetime_ = timenow;
  age_ = 0;
}

void
location::set_alive (bool alive)
{
  if (!alive && alive_) {
    dead_time_ = timenow;
  } 
  if (!alive && isme_) {
    trace << " set_alive yo me not dead " << (n_>>144) << "\n";
    return;
  }
  // Setting age is ok if we only call set_alive after direct comm
  // with this location.
  age_ = 0;
  alive_ = alive;
  updatetime_ = timenow;
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
