/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 * Copyright (C) 2001 Frans Kaashoek (kaashoek@lcs.mit.edu) and 
 *                    Frank Dabek (fdabek@lcs.mit.edu).
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

#include "chord.h"
#include "dhash_prot.h"
#include "math.h"

#define GAIN 0.2
#define MAX_REXMIT 3
#define USE_PERHOST_LATENCIES 1
#define MIN_RPC_FAILURE_TIMER 3

int seqno = 0;
u_int64_t st = getusec ();

#ifdef FAKE_DELAY
long geo_distance (chordID x, chordID y) 
{
  u_long xl = x.getui ();
  u_long yl = y.getui ();

  if (x == y) return 0;
  if ((xl & 0x1) == (yl & 0x1)) return 20;
  else return 180;
}
#endif /* FAKE_DELAY */

const int dhashtcp (getenv ("DHASHTCP") ? 1 : 0);
int sorter (const void *, const void *);

void
locationtable::doRPC (chordID &ID, 
		      rpc_program prog, int procno, 
		      ptr<void> in, void *out, aclnt_cb cb)
{

  if (dhashtcp)
    doRPC_tcp (ID, prog, procno, in, out, cb);
  else
    doRPC_udp (ID, prog, procno, in, out, cb);
}


void
locationtable::doRPC_udp (chordID &ID, 
			  rpc_program prog, int procno, 
			  ptr<void> in, void *out, aclnt_cb cb)
{

  reset_idle_timer ();
  if (left + cwind < seqno) {
    RPC_delay_args *args = New RPC_delay_args (ID, prog, procno,
					       in, out, cb, getusec ());
    enqueue_rpc (args);
  } else {
    location *l = getlocation (ID);
    assert (l);
    assert (l->refcnt >= 0);
    touch_cachedlocs (l);
    
    ref<aclnt> c = aclnt::alloc (dgram_xprt, prog, 
				 (sockaddr *)&(l->saddr));
    doRPC_issue (ID, prog, procno, in, out, cb, c);
  }
}

void
locationtable::doRPC_issue (chordID &ID, 
			    rpc_program prog, int procno, 
			    ptr<void> in, void *out, aclnt_cb cb,
			    ref<aclnt> c)
{
  /* statistics */
  nsent++;

  location *l = getlocation (ID);
  assert (l);
  assert (l->refcnt >= 0);
  touch_cachedlocs (l);
  
  rpc_state *C = New rpc_state (cb, ID, getusec (), seqno, prog.progno);
  
  C->b = rpccb_chord::alloc (c, 
			     wrap (this, &locationtable::doRPCcb, c, C),
			     wrap (this, &locationtable::timeout, C),
			     in,
			     out,
			     procno, 
			     (sockaddr *)&(l->saddr));
  
  sent_elm *s = New sent_elm (seqno);
  sent_Q.insert_tail (s);
  
  long sec, nsec;
  setup_rexmit_timer (ID, &sec, &nsec);
  C->b->send (sec, nsec);

  seqno++;
}

void
locationtable::timeout (rpc_state *C)
{
  C->s = 0;
  rpc_done (-1);
}

void
locationtable::doRPCcb (ref<aclnt> c, rpc_state *C, clnt_stat err)
{

  if (err) {
    nrpcfailed++;
    warn << "locationtable::doRPCcb: failed: " << err << "\n";
    chordnode->deletefingers (C->ID);
  } else if (C->s > 0) {
    u_int64_t lat = getusec () - C->s;
    update_latency (C->ID, lat, (C->progno == DHASH_PROGRAM));
  }

  (C->cb) (err);

  if (!c->xprt ()->reliable) {
    rpc_done (C->seqno);
    delete C;
  };
}

void
locationtable::rpc_done (long acked_seqno)
{

  if (seqno < 0) {
    update_cwind (-1);
    return;
  }

  if (acked_seqno > 0) {
    acked_seq.push_back (acked_seqno);
    acked_time.push_back ((getusec () - st)/1000000.0);
    if (acked_seq.size () > 10000) acked_seq.pop_front ();
    if (acked_time.size () > 10000) acked_time.pop_front ();
  }

  sent_elm *s = sent_Q.first;
  while (s) {
    if (s->seqno == acked_seqno) {
      sent_Q.remove (s);
      delete s;
      break;
    }
    s = sent_Q.next (s);
  }
  
  update_cwind (acked_seqno);

  while (Q.first && (left + cwind >= seqno) ) {
    int qsize = (num_qed > 100) ? 100 :  num_qed;
    int next = (int)(qsize*((float)random()/(float)RAND_MAX));
    RPC_delay_args *args =  Q.first;
    for (int i = 0; (args) && (i < next); i++)
      args = Q.next (args);
    assert (args);
    Q.remove (args);

    chordID ID = args->ID;
    doRPC (ID, args->prog,
	   args->procno, 
	   args->in,
	   args->out,
	   args->cb);
    delete args;
    num_qed--;
  }

}

void
locationtable::reset_idle_timer ()
{
  if (idle_timer) timecb_remove (idle_timer);
  idle_timer = delaycb (5, 0, wrap (this, &locationtable::idle));
}

void
locationtable::idle () 
{
  cwind = 1.0;
  ssthresh = 6;
  idle_timer = NULL;
}

void
locationtable::update_cwind (int seq) 
{

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
    ssthresh = cwind/2; //MD
    if (ssthresh < 1.0) ssthresh = 1.0;
    cwind = 1.0;

  }

  cwind_cum += cwind;
  num_cwind_samples++;
  
  cwind_cwind.push_back (cwind);
  cwind_time.push_back ((getusec () - st)/1000000.0);
  if (cwind_cwind.size () > 10000) cwind_cwind.pop_front ();
  if (cwind_time.size () > 10000) cwind_time.pop_front ();

}

void
locationtable::enqueue_rpc (RPC_delay_args *args) 
{
  num_qed++;
  Q.insert_tail (args);
}


void
locationtable::setup_rexmit_timer (chordID ID, long *sec, long *nsec)
{
#define MIN_SAMPLES 10

  float alat;

  if (nrpc > MIN_SAMPLES)
    if (avg_lat >  bf_lat + 4*bf_var) {
      alat = avg_lat;
    } else {
      alat = bf_lat + 4*bf_var;
    }
  else 
    alat = 1000000;

#ifdef USE_PERHOST_LATENCIES
  location *l = getlocation (ID);
  if (l->nrpc > MIN_SAMPLES) 
    alat = l->a_lat + 4*l->a_var;
#endif /* USE_PERHOST_LATENCIES */

  //statistics
  timers.push_back (alat);
  if (timers.size () > 1000) timers.pop_front ();

  *sec = (long)(alat/1000000);
  *nsec = ((long)alat % 1000000) * 1000;

  if (*nsec < 0 || *sec < 0)
    panic ("[send to cates@mit.edu] setup: sec %ld, nsec %ld, alat %f, avg_lat %f, bf_lat %f, bf_var %f\n",
	   *sec, *nsec, alat, avg_lat, bf_lat, bf_var);
}


void
locationtable::update_latency (chordID ID, u_int64_t lat, bool bf)
{
  location *l = getlocation (ID);
  assert (l);

  l->rpcdelay += lat;
  l->nrpc++;
  rpcdelay += lat;
  nrpc++;
  
  //update per-connection latency
  float err = (lat - l->a_lat);
  l->a_lat = l->a_lat + GAIN*err;
  if (err < 0) err = -err;
  l->a_var = l->a_var + GAIN*(err - l->a_var);
  if (lat > l->maxdelay) l->maxdelay = lat;

  //update global latency
  err = (lat - a_lat);
  a_lat = a_lat + GAIN*err;
  if (err < 0) err = -err;
  a_var = a_var + GAIN*(err - a_var);

  //update the 9xth percentile from the recent per-host averages
#define NUMLOCS 100
#define PERCENTILE 0.95

  location *cl = locs.first ();
  float host_lats [NUMLOCS];
  int i;
  for (i = 0; i < NUMLOCS && cl; i++) {
    host_lats[i] = cl->a_lat + 4*cl->a_var;
    cl = locs.next (cl);
  }
  qsort (host_lats, i, sizeof(float), &sorter);
 

  int which = (int)(PERCENTILE*i);
  avg_lat = host_lats[which];
  
  //update the recent 1K fetch latencies
  if (bf) {
    err = (lat - bf_lat);
    bf_lat = bf_lat + 0.2*err;
    if (err < 0) err = -err;
    bf_var = bf_var + 0.2*(err - bf_var);
    lat_big.push_back (lat);
    if (lat_big.size () > 1000) lat_big.pop_front ();
  } else {
    lat_little.push_back (lat);
    if (lat_little.size () > 1000) lat_little.pop_front ();
  }
}

void
locationtable::stats () 
{
  char buf[1024];

  warnx << "LOCATION TABLE STATS: estimate # nodes " << nnodes << "\n";
  warnx << "total # of RPCs: good " << nrpc << " failed " << nrpcfailed << "\n";
  sprintf(buf, "       Average latency/variance: %f/%f\n", a_lat, a_var);
  warnx << buf;
  sprintf(buf, "       Average cwind: %f\n", cwind_cum/num_cwind_samples);
  warnx << buf << "  Per link avg. RPC latencies\n";
  for (location *l = locs.first (); l ; l = locs.next (l)) {
    warnx << "    link " << l->n << " : refcnt: " << l->refcnt << " # RPCs: "
	  << l->nrpc << "\n";
    sprintf (buf, "       Average latency: %f\n"
	     "       Average variance: %f\n",
	     l->a_lat, l->a_var);
    warnx << buf;
    sprintf (buf, "       Max latency: %qd\n", l->maxdelay);
    warnx << buf;
    sprintf (buf, "       Net address: %s\n", 
	     inet_ntoa (l->saddr.sin_addr));
    warnx << buf;
  }

  warnx << "Timer history:\n";
  for (unsigned int i = 0; i < timers.size (); i++) {
    sprintf (buf, "%f", timers[i]);
    warnx << "t: " << buf << "\n";
  }

  warnx << "Latencies (little):\n";
  for (unsigned int i = 0; i < lat_little.size (); i++) {
    sprintf (buf, "%f", lat_little[i]);
    warnx << "ll: " << buf << "\n";
  }

  warnx << "Latencies (big):\n";
  for (unsigned int i = 0; i < lat_big.size (); i++) {
    sprintf (buf, "%f", lat_big[i]);
    warnx << "lb: " << buf << "\n";
  }

  warnx << "cwind over time:\n";
  for (unsigned int i = 0; i < cwind_cwind.size (); i++) {
    sprintf (buf, "%f %f", cwind_time[i], cwind_cwind[i]);
    warnx << "cw: " << buf << "\n";
  }

  warnx << "ACK trace:\n";
  for (unsigned int i = 0; i < acked_seq.size (); i++) {
    sprintf (buf, "%f", acked_time[i]);
    warnx << "at: " << buf << " " << acked_seq[i] << "\n";
  }
}

// ------------- TCP hack stuff


void
locationtable::doRPC_tcp (chordID &ID, 
			  rpc_program prog, int procno, 
			  ptr<void> in, void *out, aclnt_cb cb)
{
  location *l = getlocation (ID);
  assert (l);
  assert (l->refcnt >= 0);
  touch_cachedlocs (l);

  // hack to avoid limit on wrap()'s number of arguments
  RPC_delay_args *args = New RPC_delay_args (ID, prog, procno,
					     in, out, cb, getusec ());


  // wierd: tcpconnect wants the address in NBO, and port in HBO
  tcpconnect (l->saddr.sin_addr, ntohs (l->saddr.sin_port),
  	      wrap (this, &locationtable::doRPC_tcp_connect_cb, args));
}


void
locationtable::doRPC_tcp_connect_cb (RPC_delay_args *args, int fd)
{
  if (fd < 0) {
    warn << "locationtable: connect failed: " << strerror (errno) << "\n";
    (args->cb) (RPC_CANTSEND);
    return;
  }


  chordID ID = args->ID;

  struct linger li;
  li.l_onoff = 1;
  li.l_linger = 0;
  setsockopt (fd, SOL_SOCKET, SO_LINGER, (char *) &li, sizeof (li));

  ptr<axprt_stream> stream_xprt = axprt_stream::alloc (fd);
  assert (stream_xprt);
  ptr<aclnt> c = aclnt::alloc (stream_xprt, args->prog);

  c->call (args->procno, args->in, args->out, args->cb);
  delete args;    
}

int sorter (const void *a, const void *b) {
  float *i = (float *)a;
  float *j = (float *)b;

  if (*i < *j) return -1;
  else return 1;
}

// ------------- rpccb_chord ----------------

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

  if (nsec < 0 || sec < 0)
    panic ("[send to cates@mit.edu]: sec %ld, nsec %ld\n", sec, nsec);

  tmo = delaycb (sec, nsec, wrap (this, &rpccb_chord::timeout_cb, deleted));
  xmit (0);
}


void
rpccb_chord::timeout_cb (ptr<bool> del)
{
  if (*del) return;

  if (utmo)
    utmo ();
  
  if (rexmits > MAX_REXMIT) {
    tmo = NULL;
    timeout ();
    return;
  }  else  {
    if (nsec < 0 || sec < 0)
      panic ("1 timeout_cb: sec %ld, nsec %ld\n", sec, nsec);

    xmit (rexmits);
    if (rexmits == MAX_REXMIT) {
      sec = MIN_RPC_FAILURE_TIMER;
      nsec = 0;
    } else {
      sec *= 2;
      nsec *= 2;
      if (nsec > 1000000000) {
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
