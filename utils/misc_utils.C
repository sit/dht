#include "misc_utils.h"
#include "id_utils.h"
#include "location.h"

#include <async.h>

#define MAX_INT 0x7fffffff
const int modelnet_on (getenv("MODELNET") ? 1 : 0 );

bool
in_vector (const vec<chordID> &v, chordID N)
{
  for (unsigned int i = 0; i < v.size (); i++)
    if (v[i] == N) return true;
  return false;
}

bool
in_vector (const vec<ptr<location> > &l, chordID N)
{
  for (unsigned int i = 0; i < l.size (); i++)
    if (l[i]->id () == N) return true;
  return false;
}

str 
gettime()
{
  str buf ("");
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  buf = strbuf (" %d:%06d", int (ts.tv_sec), int (ts.tv_nsec/1000));
  return buf;
}

u_int64_t
getusec (bool syscall)
{
  static bool initialized (false);
  /*
   * Use SFS(lite)'s global time variable.
   * It is updated at least once per event loop, and optionally, here.
   */
  if (syscall || !initialized) {
    initialized = true;
    clock_gettime (CLOCK_REALTIME, &tsnow);
  } 
  return tsnow.tv_sec * INT64(1000000) + tsnow.tv_nsec / 1000;
}

float
uniform_random_f (float max)
{
  float f;
  unsigned long c = random ();
  f = 2*max*(((float)c)/((float)RAND_MAX));
  f -= max;
  return f;
}

u_int32_t
uniform_random(double a, double b)
{
  double f;
  int c = random();

  if (c == MAX_INT) c--;
  f = (b - a)*((double)c)/((double)MAX_INT);

  return (u_int32_t)(a + f);
}

chord_hostname
my_addr () {
  vec<in_addr> addrs;
  if (!myipaddrs (&addrs))
    fatal ("my_addr: cannot find my IP address.\n");

  in_addr *addr = addrs.base ();
  in_addr *loopback = 0;
  while (addr < addrs.lim ()) {
    if (ntohl (addr->s_addr) == INADDR_LOOPBACK) loopback = addr;
    else break;
    addr++;
  }
  if (addr >= addrs.lim () && (loopback == NULL))
    fatal ("my_addr: cannot find my IP address.\n");
  if (addr >= addrs.lim ()) {
    warnx << "my_addr: use loopback address as my address\n";
    addr = loopback;
  }
  str ids = inet_ntoa (*addr);
  return ids;
}

chord_node
make_chord_node (const chord_node_wire &nl)
{
  chord_node n;
  struct in_addr x;
  x.s_addr = htonl (nl.machine_order_ipv4_addr);
  if (modelnet_on) {
    x.s_addr = x.s_addr & 0xffff00ff; //JY: clear the modelnet bits
  }
  n.r.hostname = inet_ntoa (x);
  n.r.port     = nl.machine_order_port_vnnum >> 16;
  n.vnode_num  = nl.machine_order_port_vnnum & 0xFFFF;
  n.x = make_chordID (n.r.hostname, n.r.port, n.vnode_num);
  n.coords = nl.coords;
  n.e = nl.e;

  //jy
  n.budget = nl.budget;
  n.knownup = nl.knownup;
  n.age = nl.age;

  return n;
}


chord_trigger_t::~chord_trigger_t ()
{
  delaycb (0, cb);
}

ptr<chord_trigger_t> chord_trigger_t::alloc (cbv::ref cb)
{
  return New refcounted<chord_trigger_t> (cb);
}
