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

ihash<str, rpcstats, &rpcstats::key, &rpcstats::h_link> rpc_stats_tab;
u_int64_t rpc_stats_lastclear (getusec ());

static inline rpcstats *
getstats (const rpc_program &prog, int procno)
{
  const rpcgen_table *rtp;
  rtp = &prog.tbl[procno];
  assert (rtp);
  str key = strbuf ("%s:%s", prog.name, rtp->name);
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
  rpcstats *stats = getstats (prog, procno);
  stats->call (b);
}

void
track_rexmit (const rpc_program &prog, int procno, size_t b)
{
  rpcstats *stats = getstats (prog, procno);
  stats->rexmit (b);
}

void
track_reply (const rpc_program &prog, int procno, size_t b)
{
  rpcstats *stats = getstats (prog, procno);
  stats->reply (b);
}


const int CHORD_RPC_STP (0);
const int CHORD_RPC_SFSU (1);
const int CHORD_RPC_SFST (2);
const int CHORD_RPC_SFSBT (3);

int chord_rpc_style = CHORD_RPC_STP;


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
    a_lat (0.0), a_var (0.0), fd (-2), orpc (0)
{
}

// -----------------------------------------------------

rpc_manager::rpc_manager (ptr<u_int32_t> _nrcv)
  : a_lat (0.0),
    a_var (0.0),
    c_err (0.0),
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
		    cbtmo_t cb_tmo,
		    long fake_seqno /* = 0 */)
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
			 ptr<void> in, void *out, aclnt_cb cb,
			 long fake_seqno /* = 0 */)
{
  return doRPC (NULL, l, prog, procno, in, out, cb, NULL, fake_seqno);
}

void
rpc_manager::doRPCcb (aclnt_cb realcb, ptr<location> l, u_int64_t sent,
		      clnt_stat err)
{
  if (err) {
    nrpcfailed++;
    l->set_alive (false);
  } else {
    nrpc++;
    // Only update latency on successful RPC.
    // This probably includes time needed for rexmits.
    u_int64_t now = getusec ();
    // prevent overflow, caused by time reversal
    if (now >= sent) {
      u_int64_t lat = now - sent;
      update_latency (NULL, l, lat);
    } else {
      warn << "*** Ignoring timewarp: sent " << sent
	   << " > now " << now << "\n";
    }
  }
  
  (realcb) (err);
}

// -----------------------------------------------------
long
tcp_manager::doRPC (ptr<location> from, ptr<location> l,
		    const rpc_program &prog, int procno, 
		    ptr<void> in, void *out, aclnt_cb cb,
		    cbtmo_t cb_tmo,
		    long fake_seqno /* = 0 */)
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
			 ptr<void> in, void *out, aclnt_cb cb,
			 long fake_seqno /* = 0 */)
{
  return doRPC (NULL, l, prog, procno, in, out, cb, NULL, fake_seqno);
}

void
tcp_manager::remove_host (hostinfo *h) 
{
  // unnecessary SO_LINGER already set
  // tcp_abort (h->fd);
  
  if (chord_rpc_style == CHORD_RPC_SFST) {
    h->fd = -2;
    h->xp = NULL;
    while (h->connect_waiters.size ()) {
      RPC_delay_args *a =  h->connect_waiters.pop_front ();
      a->cb (RPC_CANTSEND);
      delete a;
    }
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
    if (chord_rpc_style == CHORD_RPC_SFST) {
      hi->fd = fd;
      hi->xp = axprt_stream::alloc (fd);
      assert (hi->xp);
      send_RPC (args);
      while (hi->connect_waiters.size ())
	send_RPC (hi->connect_waiters.pop_front ());
    }
    else {
      ptr<axprt_stream> xp = axprt_stream::alloc (fd);
      assert (xp);
      ptr<aclnt> c = aclnt::alloc (xp, args->prog);
      assert (c);
      c->call (args->procno, args->in, args->out, 
	       wrap (this, &tcp_manager::doRPC_tcp_cleanup, c, args));
    }
  }
}

void
tcp_manager::doRPC_tcp_cleanup (ptr<aclnt> c, RPC_delay_args *args,
                                clnt_stat err)
{
  hostinfo *hi = lookup_host (args->l->address ());
  u_int64_t diff;
  if (args->from && 
      args->from->address ().hostname == args->l->address ().hostname)
    diff = 5000;
  else {
    u_int64_t now = getusec ();
    diff = now - args->now;
    if (diff > 5000000)
      warn << "long tcp latency to " << args->l->address ().hostname
           << ": " << diff << ", orpc " << hi->orpc << "\n";
  }
  hi->orpc--;
  if (diff < 5000000)
    update_latency (NULL, args->l, diff);
  (*args->cb)(err);
  delete args;
}

void
tcp_manager::stats ()
{
  char buf[1024];
  rpc_manager::stats ();
  for (hostinfo *h = hosts.first (); h ; h = hosts.next (h)) {
    warnx << "  host " << h->host << ": rpcs " << h->nrpc
          << ", orpcs " << h->orpc;
    sprintf (buf, ", lat %.1f, var %.1f\n", h->a_lat/1000, h->a_var/1000);
    warnx << buf;
  }
}

