#include <async.h>
#include <aios.h>
#include <rxx.h>
#include <qhash.h>

#include <dbfe.h>
#include "usenet.h"
#include "newspeer.h"

// XXX should gc newspeers if connections are closed.
qhash<str, ref<newspeer> > newspeers;

void
feed_article (str id, const vec<str> &groups)
{
  ptr<peerinfo> pi (NULL);
  ptr<newspeer> np (NULL);
  for (size_t i = 0; i < opt->peers.size (); i++) {
    pi = opt->peers[i];
    np = newspeers[pi->peerkey];
    if (np == NULL) {
      np = New refcounted<newspeer> (pi->hostname, pi->port);
      newspeers.insert (pi->peerkey, np);
    }
    for (size_t j = 0; j < groups.size (); j++) {
      if (pi->desired (groups[j])) {
	np->queue_article (id);
	break;
      }
    }
  }
}

peerinfo::peerinfo (str h, u_int16_t p) :
  hostname (h),
  port (p),
  peerkey (strbuf ("%s:%d", h.cstr (), p))
{
}

peerinfo::~peerinfo ()
{
}

bool
peerinfo::add_pattern (str p)
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
peerinfo::desired (str group)
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


u_int64_t
newspeer::totalfedbytes ()
{
  u_int64_t totalout (0);
  qhash_slot<str, ref<newspeer> > *s = newspeers.first ();
  while (s) {
    totalout += s->value->fedoutbytes ();
    s = newspeers.next (s);
  }
  return totalout;
}

newspeer::newspeer (str h, u_int16_t p) :
  s (-1),
  aio (NULL),
  conncb (NULL),
  state (HELLO_WAIT),
  dhtok (false),
  streamok (false),
  fedoutbytes_ (0),
  hostname (h),
  port (p)
{
  start_feed ();
}

newspeer::~newspeer ()
{
  if (conncb) {
    timecb_remove (conncb);
    conncb = NULL;
  }
}

void
newspeer::reset ()
{
  aio = NULL; // aios destructor will close socket cleanly.
  s = -1;
  if (conncb) {
    timecb_remove (conncb);
    conncb = NULL;
  }
  state = HELLO_WAIT;
  dhtok = false;
  streamok = false;
}

void
newspeer::start_feed (int t)
{
  conncb = NULL;
  tcpconnect (hostname, port, wrap (this, &newspeer::feed_connected, t));
}

void
newspeer::feed_connected (int t, int ns)
{
  s = ns;
  if (s < 0) {
    t *= 2; if (t > 3600) t = 3600;
    warn << "Connection to " << hostname << ":" << port << " failed: "
	 << strerror (errno) << "; retry in " << t << " seconds.\n";
    conncb = delaycb (t, 0, wrap (this, &newspeer::start_feed, t));
    return;
  }
  aio = aios::alloc (s);
  aio->settimeout (opt->peer_timeout);
  aio->setdebug (strbuf("%dp", s));
  // Start lockstep
  state = HELLO_WAIT;
  aio->readline (wrap (this, &newspeer::process_line));
}

static rxx emptyrxx ("^\\s*$");
void
newspeer::process_line (const str data, int err)
{
  strbuf prefix ("%dp: ", s);
  if (err < 0) {
    warn << prefix << "newspeer aio oops " << err << "\n";
    if (err == ETIMEDOUT) {
      reset ();
      return;
    }
  }
  if (!data || !data.len()) {
    warn << prefix << "newspeer data oops\n";
    reset ();
    return;
  }

  vec<str> cmdargs;
  int n = split (&cmdargs, rxx("\\s+"), data);
  if (n > 0) {
    str code = cmdargs[0];
    if (state == HELLO_WAIT) {
      if (code == "200") {
	state = MODE_CHANGE;
	aio << "MODE STREAM\r\n";
      } else {
	warn << prefix << "Unexpected input from peer: " << data << "\n";
      }
    }
    else if (state == MODE_CHANGE) {
      state = DHT_CHECK;
      streamok = false;
      if (code[0] == '2') {
	streamok = true;
	if (code != "203")
	  warn << prefix << "Unexpected (but ok) response to mode stream: "
		<< data << "\n";
	flush_queue ();
      }
      else if (code != "500") {
	warn << prefix << "Unexpected response to mode stream: " << data << "\n";
      }
    }
    else if (state == DHT_CHECK || state == FEEDING) {
      if (code == "238") {				// CHECK, CHECKDHT
	if (state == DHT_CHECK)
	  dhtok = true;
	send_article (cmdargs[1]);
      } else if (code == "239" || code == "438") { // TAKETHIS, TAKEDHT
	// fantastic. some article went over ok.
	// XXX remove from list of articles to send
	warn << prefix << "sent successfully.\n";
      } else if (code == "431" || code == "400" || code == "439") {
	// something went wrong. try again later... XXX
	warn << prefix << "dropping to-delay article on the floor " << data << "\n";
      } else if (code == "480") {			// CHECK*, TAKE*
	// permission denied.  disconnect and go away.
	warn << prefix << "permission denied!\n";
      }
      else if (code[0] != '5') {
	warn << prefix << "Unexpected response to check/take command: " << data << "\n";
	if (state == DHT_CHECK)
	  dhtok = false;
      }
      state = FEEDING;
    }
    else
      fatal << prefix <<  "UNKNOWN STATE: " << state << "\n";
  } else if (emptyrxx.match(data)) {
    warn << prefix << "empty line\n";
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
    if (!streamok || state == DHT_CHECK)
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
  warn << hostname << ": queued " << id << "\n";
  if (!aio) {
    if (!conncb)
      start_feed ();
  }
  else
    flush_queue ();
}

void
newspeer::send_article (str id)
{
  ptr<dbrec> key, d;

  key = New refcounted<dbrec> (id, id.len ());
  d = header_db->lookup (key);
  if (!d) {
    warn << hostname << ": was going to send " << id << " but not in db?!!\n";
    return;
  }

  str header (d->value, d->len);
  if (dhtok) {
    aio << "TAKEDHT " << id << "\r\n";
    aio << header << "\r\n.\r\n"; // need extra \r\n for feed parsing other side
    fedoutbytes_ += id.len () + d->len + 15;
  } else {
    // dispatch a body fetch
    // aio << "TAKETHIS " << id << "\r\n";
    warn << hostname << ": would send " << id << " via TAKETHIS.\n";
  }
}

u_int64_t
newspeer::fedoutbytes (bool reset)
{
  u_int64_t x = fedoutbytes_;
  if (reset)
    fedoutbytes_ = 0;
  return x;
}
