#include "misc_utils.h"
#include <amisc.h>

#define MAX_INT 0x7fffffff

bool
in_vector (const vec<chordID> &v, chordID N)
{
  for (unsigned int i = 0; i < v.size (); i++)
    if (v[i] == N) return true;
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
getusec ()
{
  timeval tv;
  gettimeofday (&tv, NULL);
  return tv.tv_sec * INT64(1000000) + tv.tv_usec;
}

float
uniform_random_f (float max)
{
  float f;
  unsigned long c = random ();
  f = 2*max*(((float)c)/((float)MAX_INT));
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
