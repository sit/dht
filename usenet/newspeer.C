#include <async.h>
#include <aios.h>
#include "usenet.h"
#include "newspeer.h"

static vec<ptr<newspeer> > peers;

newspeer::newspeer (str h, u_int16_t p) :
  hostname (h),
  port (p)
{
}

newspeer::~newspeer ()
{
}

ptr<newspeer>
newspeer::alloc (str h, u_int16_t p)
{
  for (size_t i = 0; i < peers.size (); i++)
    if (peers[i]->hostname == h && peers[i]->port == p)
      return NULL;
  
  ptr<newspeer> np = New refcounted<newspeer> (h, p);
  peers.push_back (np);
  return np;
}

bool
newspeer::add_pattern (str p)
{
  const char *c = p.cstr ();
  while (*c) {
    if (!isalnum(*c) && *c != '.' && *c != '*')
      return false;
    c++;
  }
  patterns.push_back (p);
  return true;
}

bool
newspeer::desired (str group)
{
  // case sensitivity?
  for (size_t d = 0; d < patterns.size (); d++) {
    str pattern = patterns[d];
    for (size_t i = 0; i < pattern.len () && i < group.len (); i++) {
      if (pattern[i] == '*' && i == pattern.len () - 1)
	return true;
      if (pattern[i] != group[i])
	break;
    }
  }
  return false;
}

void
newspeer::start_feed (int t = 60)
{
  tcpconnect (hostname, port, wrap (this, &newspeer::feed_connected, t));
}

void
newspeer::feed_connected (int t, int s)
{
  if (s < 0) {
    t *= 2; if (t > 3600) t = 3600;
    warn << "Connection to " << hostname << ":" << port << " failed: "
	 << strerror (errno) << "; retry in " << t << "seconds.\n";
    delaycb (t, 0, wrap (this, &newspeer::start_feed, t));
    return;
  }
  aio = aios::alloc (s);
  aio->settimeout (opt->peer_timeout);
  aio << "MODE STREAM\r\n";
}
