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

#include "dhash_prot.h"
#include "chord_util.h"
#include "comm.h"
#include "location.h"

long outbytes;
ihash<str, rpcstats, &rpcstats::key, &rpcstats::h_link> rpc_stats_tab;

#define max_host_cache 1000

// UTILITY FUNCTIONS

const int shortstats (getenv ("SHORT_STATS") ? 1 : 0);
const int aclnttrace (getenv ("ACLNT_TRACE")
		      ? atoi (getenv ("ACLNT_TRACE")) : 0);
const bool aclnttime (getenv ("ACLNT_TIME"));

static inline const char *
tracetime ()
{
  static str buf ("");
  if (aclnttime) {
    timespec ts;
    clock_gettime (CLOCK_REALTIME, &ts);
    buf = strbuf (" %d.%06d", int (ts.tv_sec), int (ts.tv_nsec/1000));
  }
  return buf;
}

static
int partition (float *A, int p, int r)
{
  float x = A[p];
  int i = p - 1;
  int j = r + 1;

  while (1) {
    do {  j -= 1; } while (A[j] > x);
    do {  i += 1; } while (A[i] < x);
    if (i >= j)
      return j;
    float tmp = A[i];
    A[i] = A[j];
    A[j] = tmp;     
  }
}

static float
myselect (float *A, int p, int r, int i)
{
  if (p == r)
    return A[p];
  int q = partition (A, p, r);
  int k = q - p + 1;
  if (i <= k)
    return myselect(A, p, q, i);
  else
    return myselect(A, q+1, r, i-k);
}

// -----------------------------------------------------
rpc_state::rpc_state (ref<location> l, aclnt_cb c, u_int64_t S, long s, int p)
  : loc (l), cb (c),  s (S), progno (p), seqno (s),
    b (NULL), rexmits (0)
{
  ID = l->n;
};

// -----------------------------------------------------
hostinfo::hostinfo (const net_address &r)
  : host (r.hostname), rpcdelay (0), nrpc (0), maxdelay (0),
    a_lat (0.0), a_var (0.0), fd (-2)
{
}

// -----------------------------------------------------

rpc_manager::rpc_manager (ptr<u_int32_t> _nrcv)
  : nrpc (0), nrpcfailed (0), nsent (0), npending (0), nrcv (_nrcv)
{
  warn << "CREATED RPC MANANGER\n";
  int dgram_fd = inetsocket (SOCK_DGRAM);
  if (dgram_fd < 0) fatal << "Failed to allocate dgram socket\n";
  dgram_xprt = axprt_dgram::alloc (dgram_fd, sizeof(sockaddr), 230000);
  if (!dgram_xprt) fatal << "Failed to allocate dgram xprt\n";
}

float
rpc_manager::get_a_lat (ptr<location> l)
{
  return 0.0;
}

float
rpc_manager::get_a_var (ptr<location> l)
{
  return 0.0;
}

float
rpc_manager::get_avg_lat ()
{
  return 0.0;
}

float
rpc_manager::get_avg_var ()
{
  return 0.0;
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
rpc_manager::doRPC (ptr<location> l,
		    rpc_program prog, int procno, 
		    ptr<void> in, void *out, aclnt_cb cb,
		    long fake_seqno /* = 0 */)
{
  ref<aclnt> c = aclnt::alloc (dgram_xprt, prog, 
			       (sockaddr *)&(l->saddr));
  
  c->call (procno, in, out, wrap (this, &rpc_manager::doRPCcb, cb)); 
  return 0;
}

long
rpc_manager::doRPC_dead (ptr<location> l,
			 rpc_program prog, int procno, 
			 ptr<void> in, void *out, aclnt_cb cb,
			 long fake_seqno /* = 0 */)
{
  doRPC (l, prog, procno, in, out, cb, fake_seqno);
}

void
rpc_manager::doRPCcb (aclnt_cb realcb, clnt_stat err)
{
  if (err)
    nrpcfailed++;
  else
    nrpc++;
  
  (realcb) (err);
}

// -----------------------------------------------------
long
tcp_manager::doRPC (ptr<location> l,
		    rpc_program prog, int procno, 
		    ptr<void> in, void *out, aclnt_cb cb,
		    long fake_seqno /* = 0 */)
{
  // hack to avoid limit on wrap()'s number of arguments
  RPC_delay_args *args = New RPC_delay_args (l, prog, procno,
					     in, out, cb);

  hostinfo *hi = lookup_host (l->addr);
  if (hi->fd == -2) { //no connect initiated
    // wierd: tcpconnect wants the address in NBO, and port in HBO
    hi->fd = -1; // signal pending connect
    tcpconnect (l->saddr.sin_addr, ntohs (l->saddr.sin_port),
		wrap (this, &tcp_manager::doRPC_tcp_connect_cb, args));
  } else if (hi->fd == -1) { //connect pending, add to waiters
    cbv waitcb = wrap (this, &tcp_manager::send_RPC, args);
    hi->connect_waiters.push_back (waitcb);
  } else if (hi->fd > 0) { //already connected
    send_RPC (args);
  }
}

long
tcp_manager::doRPC_dead (ptr<location> l,
			 rpc_program prog, int procno, 
			 ptr<void> in, void *out, aclnt_cb cb,
			 long fake_seqno /* = 0 */)
{
  doRPC (l, prog, procno, in, out, cb, fake_seqno);
}

void
tcp_manager::remove_host (hostinfo *h) 
{
  warn << "closing " << h->fd << " on " << h->host << "\n";
  tcp_abort (h->fd);
  h->fd = -2;
  h->xp = NULL;
}

void
tcp_manager::send_RPC (RPC_delay_args *args)
{

  hostinfo *hi = lookup_host (args->l->addr);
  if (hi->xp->ateof()) 
    {
      hostlru.remove (hi);
      hostlru.insert_tail (hi);
      if (hi->fd > 0) close (hi->fd);
      (args->cb) (RPC_CANTSEND);
      return;
    }
  ptr<aclnt> c = aclnt::alloc (hi->xp, args->prog);
  c->call (args->procno, args->in, args->out, 
	   wrap (this, &tcp_manager::doRPC_tcp_cleanup, c, args));
}
void
tcp_manager::doRPC_tcp_connect_cb (RPC_delay_args *args, int fd)
{
  ptr<axprt_stream> stream_xprt = NULL;
  hostinfo *hi = lookup_host (args->l->addr);
  hi->fd = fd;
  if (fd < 0) {
    warn << "locationtable: connect failed: " << strerror (errno) << "\n";
    (args->cb) (RPC_CANTSEND);
    delete args;
  } else {
    struct linger li;
    li.l_onoff = 1;
    li.l_linger = 0;
    setsockopt (fd, SOL_SOCKET, SO_LINGER, (char *) &li, sizeof (li));
    tcp_nodelay (fd);
    make_async(fd);
    stream_xprt = axprt_stream::alloc (fd);
    hi->xp = stream_xprt;
    assert (stream_xprt);
    send_RPC (args);
  }

  for (unsigned int i = 0; i < hi->connect_waiters.size (); i++)
    (*hi->connect_waiters[i])();
}

void
tcp_manager::doRPC_tcp_cleanup (ptr<aclnt> c, RPC_delay_args *args, clnt_stat err)
{
  (*args->cb)(err);
  delete args;
}

// -----------------------------------------------------
stp_manager::stp_manager (ptr<u_int32_t> _nrcv)
  : rpc_manager (_nrcv),
    a_lat (0.0),
    a_var (0.0),
    avg_lat (0.0),
    rpcdelay (0),
    seqno (0),
    cwind (1.0),
    cwind_ewma (1.0),
    ssthresh (6.0),
    left (0),
    cwind_cum (0.0),
    num_cwind_samples (0),
    num_qed (0),
    idle_timer (NULL)
{
  delaycb (1, 0, wrap (this, &stp_manager::ratecb));
  reset_idle_timer ();
  st = getusec ();
  fake_seqno = -1;
}

stp_manager::~stp_manager ()
{
  if (idle_timer)
    timecb_remove (idle_timer);
}

void
stp_manager::ratecb () {
#if 0
  warnx << "sent " << nsent << " RPCs in the last second\n";
  warnx << "received " << *nrcv << " RPCs in the last second\n";
  warnx << npending << " RPCs are outstanding\n";
#endif
  // do something if nsent (+ nrcv) is too high xxx?

  delaycb (1, 0, wrap (this, &stp_manager::ratecb));
  nsent = outbytes;
  *nrcv = 0;
}

float
stp_manager::get_a_lat (ptr<location> l)
{
  hostinfo *h = lookup_host (l->addr);
  return h->a_lat;
}

float
stp_manager::get_a_var (ptr<location> l)
{
  hostinfo *h = lookup_host (l->addr);
  return h->a_var;
}

float
stp_manager::get_avg_lat ()
{
  return a_lat;
}

float
stp_manager::get_avg_var ()
{
  return a_var;
}

long
stp_manager::doRPC_dead (ptr<location> l,
			 rpc_program prog, int procno, 
			 ptr<void> in, void *out, aclnt_cb cb,
			 long fake_seqno /* = 0 */)
{
  warn << "dead RPC call\n";
    
  ref<aclnt> c = aclnt::alloc (dgram_xprt, prog, 
			       (sockaddr *)&(l->saddr));
  
  c->call (procno, in, out, cb); 
  return 0;
}

long
stp_manager::doRPC (ptr<location> l,
		    rpc_program prog, int procno, 
		    ptr<void> in, void *out, aclnt_cb cb,
		    long _fake_seqno /* = 0 */)
{
  reset_idle_timer ();
  if (left + cwind < seqno) {
    RPC_delay_args *args = New RPC_delay_args (l, prog, procno,
					       in, out, cb);
    args->fake_seqno = fake_seqno;
    enqueue_rpc (args);
    fake_seqno--;
    return args->fake_seqno;
  } else {

    ref<aclnt> c = aclnt::alloc (dgram_xprt, prog, 
				 (sockaddr *)&(l->saddr));
    rpc_state *C = New rpc_state (l, cb, getusec (), seqno, prog.progno);
    C->procno = procno;
   
    C->b = rpccb_chord::alloc (c, 
			       wrap (this, &stp_manager::doRPCcb, c, C),
			       wrap (this, &stp_manager::timeout, C),
			       in,
			       out,
			       procno, 
			       (sockaddr *)&(l->saddr));
    
    sent_elm *s = New sent_elm (seqno);
    sent_Q.insert_tail (s);
    
    long sec, nsec;
    hostinfo *h = lookup_host (l->addr);
    setup_rexmit_timer (h, &sec, &nsec);

    //    warn << getusec () << " sent an RPC destined for " << inet_ntoa (l->saddr.sin_addr) << "\n";
    C->b->send (sec, nsec);
    seqno++;
    nsent++;

    if (_fake_seqno) {
      C->rexmit_ID = _fake_seqno;
    } else {
      C->rexmit_ID = C->seqno;
    }

    user_rexmit_table.insert (C);
    return seqno;
  }
}

void
stp_manager::timeout (rpc_state *C)
{

#if 0
  u_int64_t now = getusec ();
  warn << "stp_manager::timeout\n"; 
  warn << "\t**now " << now << "\n";
  warn << "\tID " << C->ID << "\n";
  warn << "\ts " << C->s << "\n";
  warn << "\tprogno.procno " << C->progno << "." << C->procno << "\n";
  warn << "\tseqno " << C->seqno << "\n";
  warn << "\trexmits " << C->rexmits << "\n";
  warn << "\tcwind " << (int)cwind << "\n";
  warn << "\tleft " << left << "\n";
#endif

  C->s = 0;
  C->rexmits++;

  //  remove_from_sentq (C->seqno);
  update_cwind (-1);
  //  rpc_done (C->seqno);
}

void
stp_manager::doRPCcb (ref<aclnt> c, rpc_state *C, clnt_stat err)
{
  if (err) {
    nrpcfailed++;
    warn << "RPC failure: " << err << " destined for " << C->ID << "\n";
  } else {
    if (C->s > 0) {
      u_int64_t now = getusec ();
      // prevent overflow, caused by time reversal
      if (now >= C->s) {
	u_int64_t lat = now - C->s;
	update_latency (C->loc, lat, (C->progno == DHASH_PROGRAM));
      } else {
	warn << "*** Ignoring timewarp: C->s " << C->s << " > now " << now << "\n";
	warnx << " " << now       << "\n";
	warnx << " " << getusec() << "\n";
	warnx << " " << getusec() << "\n";
	warnx << " " << getusec() << "\n";
	warnx << " " << getusec() << "\n";
	warnx << " " << getusec() << "\n";
	warnx << " " << getusec() << "\n";
	warnx << " " << getusec() << "\n";
	warnx << " " << getusec() << "\n";
	warnx << " " << getusec() << "\n";
      }
    }
  }

  user_rexmit_table.remove (C);
  (C->cb) (err);
  //  if (C->s > 0) 
    remove_from_sentq (C->seqno);
  update_cwind (C->seqno);
  rpc_done (C->seqno);
  delete C;
}

void
stp_manager::remove_from_sentq (long acked_seqno)
{
  
  // Ideally sent_Q should be a lru ordered hash table.
  // this might clean up the code slight and provide
  // a slight performance boost
  // --josh
  sent_elm *s = sent_Q.first;
  while (s) {
    if (s->seqno == acked_seqno) {
      sent_Q.remove (s);
      delete s;
      break;
    }
    s = sent_Q.next (s);
  }

}
void
stp_manager::rpc_done (long acked_seqno)
{

#ifdef __JJ__
  warnx << gettime() << " rpc_done " << 1 + acked_seqno << "\n";
#endif

  
  while (Q.first && (left + cwind >= seqno) ) {
    RPC_delay_args *args = Q.first;

#ifdef RANDOMIZE_STP_Q
    int qsize = (num_qed > 100) ? 100 :  num_qed;
    int next = (int)(qsize*((float)random()/(float)RAND_MAX));
    for (int i = 0; (args) && (i < next); i++)
      args = Q.next (args);
#endif

    //stats
    long now = getusec ();
    long diff = now - args->now;
    lat_inq.push_back (diff);
    if (lat_inq.size () > 1000) lat_inq.pop_back ();

    assert (args);
    Q.remove (args);

    doRPC (args->l, args->prog,
	   args->procno, 
	   args->in,
	   args->out,
	   args->cb,
	   args->fake_seqno);
    delete args;
    num_qed--;
  }

}

void
stp_manager::reset_idle_timer ()
{
  if (idle_timer) timecb_remove (idle_timer);
  idle_timer = delaycb (5, 0, wrap (this, &stp_manager::idle));
}

void
stp_manager::idle () 
{
  cwind = 1.0;
  cwind_ewma = 1.0;
  ssthresh = 6;
  idle_timer = NULL;
}

void
stp_manager::rexmit (long seqno)
{
  rpc_state *C = user_rexmit_table[seqno];
  if (!C) {
    warn << seqno << "retransmit ID  not present for retransmission\n";
    return;
  }
  C->b->user_rexmit ();
}
void
stp_manager::update_cwind (int seq) 
{
  float oldcwind = cwind;
  if (seq >= 0) {
    if (cwind < ssthresh) 
      cwind += 1.0; //slow start
    else
      cwind += 1.0/cwind; //AI
    if (seq == left) {
      if (sent_Q.first) 
	left = sent_Q.first->seqno;
      else
	left = seqno;
    }
  } else {
    ssthresh = cwind_ewma/2; // MD
    if (ssthresh < 1.0) ssthresh = 1.0;
    //    cwind = cwind/2;
    // if (cwind < 1.0) cwind = 1.0;
    cwind = 1.0;

  }

  char buf[128];
  sprintf (buf, "old cwind: %f, new %f\n", oldcwind, cwind);
  cwind_ewma = (cwind_ewma*49 + cwind)/50;
  cwind_cum += cwind;
  num_cwind_samples++;
  
  //stats
  cwind_cwind.push_back (cwind);
  cwind_time.push_back ((getusec () - st)/1000000.0);
  if (cwind_cwind.size () > 1000) cwind_cwind.pop_front ();
  if (cwind_time.size () > 1000) cwind_time.pop_front ();
  
#ifdef PEG_CWIND
  cwind = 100.0; 
#endif
}

void
stp_manager::enqueue_rpc (RPC_delay_args *args) 
{
  num_qed++;
  qued_hist.push_back (num_qed);
  if (qued_hist.size () > 1000) qued_hist.pop_back ();
  Q.insert_tail (args);
}


void
stp_manager::setup_rexmit_timer (hostinfo *h, long *sec, long *nsec)
{
#define MIN_SAMPLES 10

  float alat;
  
  if (nrpc > MIN_SAMPLES) {
    alat = 1.5*avg_lat;
  }
  else 
    alat = 1000000;

#ifdef PERHOST
  if (h && h->nrpc > MIN_SAMPLES) {
    alat = h->a_lat + 4*h->a_var;
    alat *= 1.5;
  }
#endif

  //statistics
  timers.push_back (alat);
  if (timers.size () > 1000) timers.pop_front ();
  
  *sec = (long)(alat / 1000000);
  *nsec = ((long)alat % 1000000) * 1000;
  
  if (*nsec < 0 || *sec < 0) {
    stats ();
    panic ("[send to cates@mit.edu] setup: sec %ld, nsec %ld, alat %f, avg_lat %f\n", *sec, *nsec, alat, avg_lat);
  }
}

void
stp_manager::update_latency (ptr<location> l, u_int64_t lat, bool bf)
{
  //do the gloal latencies
  rpcdelay += lat;
  nrpc++;

  //update global latency
  float err = (lat - a_lat);
  a_lat = a_lat + GAIN*err;
  if (err < 0) err = -err;
  a_var = a_var + GAIN*(err - a_var);

  //update the 9xth percentile latencies from the recent per-host averages
#define PERCENTILE 0.98
  hostinfo *cur = hosts.first ();
  float host_lats [max_host_cache];
  size_t i;
  for (i = 0; i < max_host_cache && cur; i++) {
    host_lats[i] = cur->a_lat + 4*cur->a_var;
    cur = hosts.next (cur);
  }
  int which = (int)(PERCENTILE*i);
  avg_lat = myselect (host_lats, 0, i - 1, which + 1);

  lat_history.push_back (avg_lat);
  if (lat_history.size () > 1000) lat_history.pop_front ();
  
  //update per-host latency
  hostinfo *h = lookup_host (l->addr);
  if (h) {
    h->rpcdelay += lat;
    h->nrpc++;
    err = (lat - h->a_lat);
    h->a_lat = h->a_lat + GAIN*err;
    if (err < 0) err = -err;
    h->a_var = h->a_var + GAIN*(err - h->a_var);
    if (lat > h->maxdelay) h->maxdelay = lat;
  }

}

void
rpc_manager::stats () 
{
  warnx << "RPC MANAGER STATS:\n";
  warnx << "total # of RPCs: good " << nrpc
	<< " failed " << nrpcfailed << "\n";
}

void stp_manager::stats ()
{
  char buf[1024];

  rpc_manager::stats ();
  
  sprintf(buf, "       Average latency/variance: %f/%f\n", a_lat, a_var);
  warnx << buf;
  sprintf(buf, "       Average cwind: %f\n", cwind_cum/num_cwind_samples);
  warnx << buf << "  Per link avg. RPC latencies\n";

  for (hostinfo *h = hosts.first (); h ; h = hosts.next (h)) {
    warnx << "    host " << h->host
	  << " # RPCs: " << h->nrpc << "\n";
    sprintf (buf, "       Average latency: %f\n"
	     "       Average variance: %f\n",
	     h->a_lat, h->a_var);
    warnx << buf;
    sprintf (buf, "       Max latency: %qd\n", h->maxdelay);
    warnx << buf;
  }
  if (shortstats) return;
  warnx << "Timer history:\n";
  for (unsigned int i = 0; i < timers.size (); i++) {
    sprintf (buf, "%f", timers[i]);
    warnx << "t: " << buf << "\n";
  }

  warnx << "Latencies:\n";
  for (unsigned int i = 0; i < lat_history.size (); i++) {
    sprintf (buf, "%f", lat_history[i]);
    warnx << "lat: " << buf << "\n";
  }

  warnx << "Latencies (in q):\n";
  for (unsigned int i = 0; i < lat_inq.size (); i++) {
    warnx << "lat(q): " << lat_inq[i] << "\n";
  }

  warnx << "cwind over time:\n";
  for (unsigned int i = 0; i < cwind_cwind.size (); i++) {
    sprintf (buf, "%f %f", cwind_time[i], cwind_cwind[i]);
    warnx << "cw: " << buf << "\n";
  }

  warnx << "queue length over time:\n";
  for (unsigned int i = 0; i < qued_hist.size (); i++) {
    sprintf (buf, "%f %ld", cwind_time[i], qued_hist[i]);
    warnx << "qued: " << buf << "\n";
  }

  warnx << "current RPCs queued for transmission pending window: " << num_qed << "\n";
  RPC_delay_args *args = Q.first;
  const rpcgen_table *rtp;
  u_int64_t now = getusec ();
  while (args) {
    rtp = &(args->prog.tbl[args->procno]);
    assert (rtp);
    long diff = now - args->now;
    warn << "  " << args->prog.name << "." << rtp->name << " for " << args->l->n << " queued for " << diff << "\n";
    args = Q.next (args);
  }

  warnx << "per program bytes\n";
  rpcstats *s = rpc_stats_tab.first ();
  while (s) {
    warnx << "  " << s->key << "\n";
    warnx << "    bytes: " << s->bytes << "\n";
    warnx << "    calls: " << s->calls << "\n";
    s = rpc_stats_tab.next (s);
  }
}

// ------------- rpccb_chord ----------------


// Stolen from aclnt::init_call
static void
printreply (aclnt_cb cb, str name, void *res,
	    void (*print_res) (const void *, const strbuf *, int,
			       const char *, const char *),
	    clnt_stat err)
{
  if (aclnttrace >= 3) {
    if (err)
      warn << "ACLNT_TRACE:" << tracetime () 
	   << " reply " << name << ": " << err << "\n";
    else if (aclnttrace >= 4) {
      warn << "ACLNT_TRACE:" << tracetime ()
	   << " reply " << name << "\n";
      if (aclnttrace >= 5 && print_res)
	print_res (res, NULL, aclnttrace - 4, "REPLY", "");
    }
  }
  (*cb) (err);
}

rpccb_chord *
rpccb_chord::alloc (ptr<aclnt> c,
		    aclnt_cb cb,
		    cbv u_tmo,
		    ptr<void> in,
		    void *out,
		    int procno,
		    struct sockaddr *dest) {

  xdrsuio x (XDR_ENCODE);
  rpc_program prog = c->rp;
  
  if (!aclnt::marshal_call (x, authnone_create (), prog.progno, 
			    prog.versno, procno, 
			    prog.tbl[procno].xdr_arg,
			    in)) {
      return NULL;
  }
  
  assert (x.iov ()[0].iov_len >= 4);
  u_int32_t &xid = *reinterpret_cast<u_int32_t *> (x.iov ()[0].iov_base);
  if (!c->xi->xh->reliable || cb != aclnt_cb_null) {
    u_int32_t txid;
    while (c->xi->xidtab[txid = (*next_xid) ()]);
    xid = txid;
  }

  // per program/proc RPC stats
  str key;
  const rpcgen_table *rtp;
  rtp = &prog.tbl[procno];
  assert (rtp);
  key = strbuf ("%s:%s", prog.name, rtp->name);

  //  str key = strbuf () << prog.progno << ":" << procno;
  rpcstats *stats = rpc_stats_tab[key];
  if (!stats) {
    stats = New rpcstats ();
    stats->key = key;
    stats->calls = 0;
    stats->bytes = 0;
    rpc_stats_tab.insert (stats);
  }
  suio *s = x.uio ();
  stats->bytes += s->resid ();
  stats->calls++;
  outbytes += s->resid ();

  // Stolen (mostly) from aclnt::init_call
  if (aclnttrace >= 2) {
    str name;
    const rpcgen_table *rtp;
    rtp = &prog.tbl[procno];
    assert (rtp);
    name = strbuf ("%s:%s x=%x", prog.name, rtp->name, xid);

    warn << "ACLNT_TRACE:" << tracetime () << " call " << name << "\n";
    if (aclnttrace >= 5 && rtp->print_arg)
      rtp->print_arg (in, NULL, aclnttrace - 4, "ARGS", "");
    if (aclnttrace >= 3 && cb != aclnt_cb_null)
      cb = wrap (printreply, cb, name, out, rtp->print_res);
  }
  
  ptr<bool> deleted  = New refcounted<bool> (false);
  rpccb_chord *ret = New rpccb_chord (c,
				      x,
				      cb,
				      u_tmo,
				      out,
				      prog.tbl[procno].xdr_res,
				      dest,
				      deleted);
  
  return ret;
}

void
rpccb_chord::send (long _sec, long _nsec) 
{
  rexmits = 0;
  sec = _sec;
  nsec = _nsec;

#ifdef TIMEOUT_FUDGING
  if (sec < 2) {
    sec = 1;
    nsec = 0;
  }
#endif

  if (nsec < 0 || sec < 0)
    panic ("[send to cates@mit.edu]: sec %ld, nsec %ld\n", sec, nsec);

  tmo = delaycb (sec, nsec, wrap (this, &rpccb_chord::timeout_cb, deleted));
#if 0
  warn ("%s xmited %d:%06d\n", gettime().cstr(), int (sec), int (nsec/1000));
#endif
  xmit (0);
}


void 
rpccb_chord::user_rexmit ()
{
  //this function does an immediate retransmit.
  //it doesn't cost you a rexmit counter bump
  //and doesn't bump the exponential either (but does reset the timeout timer)
  timecb_remove (tmo);
  xmit (rexmits);
  tmo = delaycb (sec, nsec, wrap (this, &rpccb_chord::timeout_cb, deleted));
}

void
rpccb_chord::timeout_cb (ptr<bool> del)
{
  if (*del) return;

  if (utmo)
    utmo ();

  if (rexmits > MAX_REXMIT) {

#if 0
    u_int64_t now = getusec ();
    warn << "rpccb_chord::timeout_cb\n"; 
    warn << "\t**now " << now << "\n";
    warn << "\trexmits " << rexmits << "\n";
#endif
    tmo = NULL;
    timeout ();
    return;
  } else {
    if (nsec < 0 || sec < 0)
      panic ("1 timeout_cb: sec %ld, nsec %ld\n", sec, nsec);

    sockaddr_in *s = (sockaddr_in *)dest;

    warnx << gettime() << " REXMIT " << xid
	  << " rexmits " << rexmits << ", timeout "<< sec << ":" << nsec << " destined for " << inet_ntoa (s->sin_addr) << "\n";

    xmit (rexmits);
    if (rexmits == MAX_REXMIT) {
      // XXX
      // The intent of this code path is to do a conservative last
      // ditch effort. However, if backed off value in <sec,nsec>
      // exceeds MIN_RPC_FAILURE_TIMER then this doesn't happen.
      // --josh
      sec = MIN_RPC_FAILURE_TIMER;
      nsec = 0;
    } else {
      sec *= 2;
      nsec *= 2;
      while (nsec >= 1000000000) {
	nsec -= 1000000000;
	sec += 1;
      }
    }
  }

  if (nsec < 0 || sec < 0)
    panic ("timeout_cb: sec %ld, nsec %ld\n", sec, nsec);

  tmo = delaycb (sec, nsec, wrap (this, &rpccb_chord::timeout_cb, deleted));
  rexmits++;
}


void
rpccb_chord::finish_cb (aclnt_cb cb, ptr<bool> del, clnt_stat err) 
{
  *del = true;
  cb (err);
}
