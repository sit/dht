#include <async.h>
#include <aios.h>
#include <rxx.h>
#include "usenet.h"
#include "newspeer.h"

static vec<ptr<newspeer> > peers;

newspeer::newspeer (str h, u_int16_t p) :
  hostname (h),
  port (p),
  s (-1),
  aio (NULL),
  state (HELLO_WAIT),
  dhtok (false),
  streamok (false)
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
  // Start lockstep
  state = HELLO_WAIT;
  aio->readline (wrap (this, &newspeer::process_line));
}

static rxx stattxtrx ("^(\\d+)\\s+(.*)$");
static rxx emptyrxx ("^\\s*$");
void
newspeer::process_line (const str data, int err)
{
  if (err < 0) {
    warnx << "newspeer aio oops " << err << "\n";
    if (err == ETIMEDOUT) {
      delete this;
      return;
    }
  }
  if (!data || !data.len()) {
    warnx << "newspeer data oops\n";
    delete this;
    return;
  }
  if (stattxtrx.match (data)) {
    str code = stattxtrx[1];
    if (state == HELLO_WAIT) {
      if (code == "200") {
	state = MODE_CHANGE;
	aio << "MODE STREAM\r\n";
      } else {
	warnx << s << ": Unexpected input from peer: " << data << "\n";
      }
    }
    else if (state == MODE_CHANGE) {
      state = DHT_CHECK;
      streamok = false;
      if (code[0] == '2') {
	streamok = true;
	if (code != "203")
	  warnx << s << ": Unexpected (but ok) response to mode stream: "
		<< data << "\n";
      }
      else if (code != "500") {
	warnx << s << ": Unexpected response to mode stream: " << data << "\n";
      }
    }
    else if (state == DHT_CHECK || state == FEEDING) {
      state = FEEDING;
      if (code == "238") {				// CHECK, CHECKDHT
	dhtok = true;
	// send_article (stattxtrx[2]); // XXX
      } else if (code == "239" || code == "438") { // TAKETHIS, TAKEDHT
	// fantastic. some article went over ok.
	// XXX remove from list of articles to send
      } else if (code == "431" || code == "400" || code == "439") {
	// something went wrong. try again later... XXX
	warnx << s << ": dropping to-delay article on the floor " << data << "\n";
      } else if (code == "480") {			// CHECK*, TAKE*
	// permission denied.  disconnect and go away.
	warnx << s << ": permission denied!\n";
      }
      else if (code[0] != '5') {
	warnx << s << ": Unexpected response to mode stream: " << data << "\n";
      }
    }
    else
      fatal << s << ": UNKNOWN STATE: " << state << "\n";
  } else if (emptyrxx.match(data)) {
    warnx << "empty line\n";
  } else {
    // XXX what is correct error code?
    aio << "500 What?\r\n";
  }
  
  aio->readline (wrap (this, &newspeer::process_line));
  // XXX
}

void
newspeer::flush_queue ()
{
  while (outq.size ()) {
    str id = outq.pop_front ();
    if (dhtok) {
      aio << "CHECKDHT " << id << "\r\n";
    } else {
      aio << "CHECK " << id << "\r\n";
    } 
    if (!streamok)
      break;
  }
}

void
newspeer::queue_article (str id)
{
  if (outq.size() > opt->peer_max_queue) {
    outq.pop_front ();
  }
  outq.push_back (id);
}
