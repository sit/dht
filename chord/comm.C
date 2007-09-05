/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 * Copyright (C) 2001 Frans Kaashoek (kaashoek@lcs.mit.edu) and 
 *                    Frank Dabek (fdabek@lcs.mit.edu).
 * Copyright (C) 2001 Frans Kaashoek (kaashoek@lcs.mit.edu), 
 *                    Frank Dabek (fdabek@lcs.mit.edu) and
 *                    Emil Sit (sit@lcs.mit.edu).
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */


// Chord's RPC manager is designed to perform flow control on all
// Chord RPCs. It maintains a cache of hosts (i.e. IP addresses)
// that it has connected to and maintains statistics about latency
// and such to those hosts. It uses these statistics to calculate
// optimal window sizes and delays.

// #define __J__ 1

#include <crypt.h>
#include <chord_prot.h>
#include <misc_utils.h>
#include "comm.h"
#include <location.h>
#include "modlogger.h"
#include "coord.h"

chord_rpc_style_t chord_rpc_style (CHORD_RPC_STP);

ihash<str, rpcstats, &rpcstats::key, &rpcstats::h_link> rpc_stats_tab;
u_int64_t rpc_stats_lastclear (getusec ());

static inline rpcstats *
getstats (int progno, int procno)
{
  str key = strbuf ("%d:%d", progno, procno);
  rpcstats *stats = rpc_stats_tab[key];
  if (!stats) {
    stats = New rpcstats (key);
    rpc_stats_tab.insert (stats);
  }
  return stats;
}

void
track_call (const rpc_program &prog, int procno, size_t b)
{
  rpcstats *stats = getstats (prog.progno, procno);
  stats->ncall++;
  stats->call_bytes += b;
}

void
track_rexmit (const rpc_program &prog, int procno, size_t b)
{
  rpcstats *stats = getstats (prog.progno, procno);
  stats->nrexmit++;
  stats->rexmit_bytes += b;
}

void
track_rexmit (int progno, int procno, size_t b)
{
  rpcstats *stats = getstats (progno, procno);
  stats->nrexmit++;
  stats->rexmit_bytes += b;
}

void
track_reply (const rpc_program &prog, int procno, size_t b)
{
  rpcstats *stats = getstats (prog.progno, procno);
  stats->nreply++;
  stats->reply_bytes += b;
}

void
track_proctime (const rpc_program &prog, int procno, u_int64_t l)
{
  rpcstats *stats = getstats (prog.progno, procno);
  // Don't bother with floats; this comes from getusec which is
  // probably more resolution that actually makes much sense anyway.
  if (stats->latency_ewma == 0)
    stats->latency_ewma = l;
  else
    stats->latency_ewma = (9 * stats->latency_ewma + l) / 10;
}


// -----------------------------------------------------
rpc_state::rpc_state (ptr<location> from, ref<location> l, aclnt_cb c, 
		      cbtmo_t _cb_tmo, long s, int p, void *out)
  : loc (l), from (from), cb (c), progno (p), seqno (s),
    b (NULL), rexmits (0), cb_tmo (_cb_tmo), out (out)
{
  ID = l->id ();
  in_window = true;
};

// -----------------------------------------------------
hostinfo::hostinfo (const net_address &r)
  : host (r.hostname), nrpc (0), maxdelay (0),
    a_lat (0.0), a_var (0.0), fd (-2), orpc (0),
    connect_time (0), last_time (0), last_sent (0), last_bw (0), bwcb (NULL)
{
  update_bw ();
}

hostinfo::~hostinfo ()
{
  if (bwcb) {
    timecb_remove (bwcb);
    bwcb = NULL;
  }
}

void
hostinfo::update_bw ()
{
  if (connect_time && xp) {
    u_int64_t now  = getusec ();
    u_int64_t sent = xp->get_raw_bytes_sent ();
    last_bw   = (sent - last_sent) / ((now - last_time)/1000000);
    last_sent = sent;
    last_time = now;
  }
  bwcb = delaycb (10, wrap (this, &hostinfo::update_bw));
}

// -----------------------------------------------------
const float rpc_manager::GAIN (0.2);

rpc_manager::rpc_manager (ptr<u_int32_t> _nrcv)
  : a_lat (0.0),
    a_var (0.0),
    c_err (0.0),
    c_err_rel (0.0),
    c_var (0.0),
    nrpc (0), nrpcfailed (0), nsent (0), npending (0), nrcv (_nrcv)
{
  warn << "CREATED RPC MANAGER\n";
  int dgram_fd = inetsocket (SOCK_DGRAM);
  if (dgram_fd < 0) fatal << "Failed to allocate dgram socket\n";
  dgram_xprt = axprt_dgram::alloc (dgram_fd, sizeof(sockaddr), 230000);
  if (!dgram_xprt) fatal << "Failed to allocate dgram xprt\n";

  next_xid = &random_getword;
}

void
rpc_manager::stats (const strbuf &ob) 
{
  char buf[1024];

  ob << "RPC MANAGER STATS:\n";
  ob << "total # of RPCs: good " << nrpc
     << " failed " << nrpcfailed << "\n";

  ob << "  Per link avg. RPC latencies\n";
  for (hostinfo *h = hosts.first (); h ; h = hosts.next (h)) {
    ob << "    host " << h->host
       << " # RPCs: " << h->nrpc
       << " (" << h->orpc << " outstanding)\n";
    if (h->connect_time && h->xp) {
      u_int64_t bytes = h->xp->get_raw_bytes_sent ();
      u_int64_t now   = getusec ();
      ob << "       Average b/w: "
	 << h->last_bw
	 <<  " "
	 << bytes / ((now - h->connect_time)/1000000)
	 << "\n";
    }

    sprintf (buf,
	     "       Average latency: %f\n"
	     "       Average variance: %f\n",
	     h->a_lat, h->a_var);
    ob << buf;
    sprintf (buf, "       Max latency: %qd\n", h->maxdelay);
    ob << buf;
  }
}

float
rpc_manager::get_a_lat (ptr<location> l)
{
  hostinfo *h = lookup_host (l->address ());
  return h->a_lat;
}

float
rpc_manager::get_a_var (ptr<location> l)
{
  hostinfo *h = lookup_host (l->address ());
  return h->a_var;
}

void
rpc_manager::remove_host (hostinfo *h)
{
}

hostinfo *
rpc_manager::lookup_host (const net_address &r)
{
  str key = strbuf () << r.hostname << ":" << r.port << "\n";
  hostinfo *h = hosts[key];
  if (!h) {
    if (hosts.size () > max_host_cache) {
      hostinfo *o = hostlru.first;
      hostlru.remove (o);
      hosts.remove (o);
      remove_host (o);
      delete (o);
    }
    h = New hostinfo (r);
    h->key = key;
    hostlru.insert_tail (h);
    hosts.insert (h);
  } else {
    // record recent access
    hostlru.remove (h);
    hostlru.insert_tail (h);
  }
  assert (h);
  return h;
}

long
rpc_manager::doRPC (ptr<location> from, ptr<location> l,
		    const rpc_program &prog, int procno, 
		    ptr<void> in, void *out, aclnt_cb cb,
		    cbtmo_t cb_tmo)
{
  ref<aclnt> c = aclnt::alloc (dgram_xprt, prog, 
			       (sockaddr *)&(l->saddr ()));

  // Make sure that there is an entry in the table for this guy.
  (void) lookup_host (l->address ());
  
  u_int64_t sent = getusec ();
  c->call (procno, in, out,
	   wrap (this, &rpc_manager::doRPCcb, cb, l, sent)); 
  return 0;
}

long
rpc_manager::doRPC_dead (ptr<location> l,
			 const rpc_program &prog, int procno, 
			 ptr<void> in, void *out, aclnt_cb cb)
{
  return doRPC (NULL, l, prog, procno, in, out, cb, NULL);
}

void
rpc_manager::doRPCcb (aclnt_cb realcb, ptr<location> l, u_int64_t sent,
		      clnt_stat err)
{
  if (err) {
    nrpcfailed++;
    l->set_alive (false);
  } else {
    count_rpc (l);
    // Only update latency on successful RPC.
    // This probably includes time needed for rexmits.
    update_latency (NULL, l, sent);
  }
  
  (realcb) (err);
}

void
rpc_manager::count_rpc (ptr<location> l, hostinfo *h)
{
  nrpc++;
  l->inc_nrpc ();
  if (!h)
    h = lookup_host (l->address ());
  if (h)
    h->nrpc++;
}

void
rpc_manager::update_latency (ptr<location> from, ptr<location> l, u_int64_t senttime)
{
  u_int64_t now = getusec ();
  // prevent overflow, caused by time reversal
  if (now < senttime) {
    warn << "*** Ignoring timewarp: sent " << senttime
	 << " > now " << now << "\n";
    return;
  }

  u_int64_t lat = now - senttime;

  //update global latency
  float err = (lat - a_lat);
  a_lat = a_lat + GAIN*err;
  if (err < 0) err = -err;
  a_var = a_var + GAIN*(err - a_var);

  //update per-host latency
  hostinfo *h = lookup_host (l->address ());
  if (h) {
    if (h->a_lat == 0 && h->a_var == 0)
      h->a_lat = lat;
    else {
      err = (lat - h->a_lat);
      h->a_lat = h->a_lat + GAIN*err;
      if (err < 0) err = -err;
      h->a_var = h->a_var + GAIN*(err - h->a_var);
    }
    if (lat > h->maxdelay) h->maxdelay = lat;

    // Copy info over to just this location
    l->set_distance (h->a_lat);
    l->set_variance (h->a_var);
  }

  //do the coordinate variance if available
  if (from && l && from->coords ().size () > 0 && l->coords ().size () > 0) {
    float predicted = Coord::distance_f (from->coords (), l->coords ());
    float sample_err = (lat - predicted);
    

    /*    
    warn << "To " << l->id () << " " << (int)sample_err << " " << (int)lat 
	 << " " << (int)predicted << " " 
	 << (int)c_err << " " << (int)c_var << " " 
	 << (int)(c_err_rel*1000) << "\n";
    */
    
    if (sample_err < 0) sample_err = -sample_err;
    float rel_err = sample_err/lat;

    c_err = (c_err*49 + sample_err)/50;
    c_err_rel = (c_err_rel*49 + rel_err)/50;
    c_var = c_var + GAIN*(sample_err - c_var);
   }
}


// -----------------------------------------------------
long
tcp_manager::doRPC (ptr<location> from, ptr<location> l,
		    const rpc_program &prog, int procno, 
		    ptr<void> in, void *out, aclnt_cb cb,
		    cbtmo_t cb_tmo)
{
  // hack to avoid limit on wrap()'s number of arguments
  RPC_delay_args *args = New RPC_delay_args (from, l, prog, procno,
					     in, out, cb, NULL);
  if (chord_rpc_style == CHORD_RPC_SFSBT) {
    tcpconnect (l->saddr ().sin_addr, ntohs (l->saddr ().sin_port),
		wrap (this, &tcp_manager::doRPC_tcp_connect_cb, args));
  } else {
    hostinfo *hi = lookup_host (l->address ());
    if (hi->fd == -2) { //no connect initiated
      // weird: tcpconnect wants the address in NBO, and port in HBO
      hi->fd = -1; // signal pending connect
      tcpconnect (l->saddr ().sin_addr, ntohs (l->saddr ().sin_port),
		  wrap (this, &tcp_manager::doRPC_tcp_connect_cb, args));
    } else if (hi->fd == -1) { //connect pending, add to waiters
      hi->connect_waiters.push_back (args);
    } else if (hi->fd > 0) { //already connected
      send_RPC (args);
    }

  }
  return 0;
}

long
tcp_manager::doRPC_dead (ptr<location> l,
			 const rpc_program &prog, int procno, 
			 ptr<void> in, void *out, aclnt_cb cb)
{
  return doRPC (NULL, l, prog, procno, in, out, cb, NULL);
}

void
tcp_manager::remove_host (hostinfo *h) 
{
  // unnecessary SO_LINGER already set
  // tcp_abort (h->fd);
  
  h->fd = -2;
  h->xp = NULL;
  h->connect_time = 0;
  while (h->connect_waiters.size ()) {
    RPC_delay_args *a =  h->connect_waiters.pop_front ();
    a->cb (RPC_CANTSEND);
    delete a;
    }
}

void
tcp_manager::send_RPC (RPC_delay_args *args)
{

  hostinfo *hi = lookup_host (args->l->address ());
  if (!hi->xp) {
    delaycb (0, 0, wrap (this, &tcp_manager::send_RPC_ateofcb, args));
  }
  else if (hi->xp->ateof()) {
    hostlru.remove (hi);
    hostlru.insert_tail (hi);
    args->l->set_alive (false);
    remove_host (hi);
    delaycb (0, 0, wrap (this, &tcp_manager::send_RPC_ateofcb, args));
  }
  else {
    hi->orpc++;
    args->now = getusec ();
    ptr<aclnt> c = aclnt::alloc (hi->xp, args->prog);
    c->call (args->procno, args->in, args->out, 
	     wrap (this, &tcp_manager::doRPC_tcp_cleanup, c, args));
  }
}

void
tcp_manager::send_RPC_ateofcb (RPC_delay_args *args)
{
  (args->cb) (RPC_CANTSEND);
  delete args;
}

void
tcp_manager::doRPC_tcp_connect_cb (RPC_delay_args *args, int fd)
{

  hostinfo *hi = lookup_host (args->l->address ());
  if (fd < 0) {
    warn << "locationtable: connect failed: " << strerror (errno) << "\n";
    (args->cb) (RPC_CANTSEND);
    args->l->set_alive (false);
    remove_host (hi);
    delete args;
  }
  else {
    struct linger li;
    li.l_onoff = 1;
    li.l_linger = 0;
    setsockopt (fd, SOL_SOCKET, SO_LINGER, (char *) &li, sizeof (li));
    tcp_nodelay (fd);
    make_async(fd);
    hi->connect_time = getusec ();
    hi->fd = fd;
    hi->xp = axprt_stream::alloc (fd, 260*1024);
    assert (hi->xp);
    send_RPC (args);
    while (hi->connect_waiters.size ())
      send_RPC (hi->connect_waiters.pop_front ());
  }
}

void
tcp_manager::doRPC_tcp_cleanup (ptr<aclnt> c, RPC_delay_args *args,
                                clnt_stat err)
{
  hostinfo *hi = lookup_host (args->l->address ());
  if (err) { 
    nrpcfailed++;
  } else {
    count_rpc (args->l, hi);
    update_latency (args->from, args->l, args->now);
  }
  if (hi) hi->orpc--;
  (*args->cb)(err);
  delete args;
}
