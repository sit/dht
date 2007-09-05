
#include <crypt.h>
#include <chord_prot.h>
#include <misc_utils.h>
#include "comm.h"
#include <location.h>
#include "modlogger.h"
#include "coord.h"
#include <transport_prot.h>

long outbytes;
const int shortstats (getenv ("SHORT_STATS") ? 1 : 0);


#define CWIND_MULT 5

stp_manager::stp_manager (ptr<u_int32_t> _nrcv)
  : rpc_manager (_nrcv),
    seqno (0),
    cwind (1.0),
    cwind_ewma (1.0),
    ssthresh (6.0),
    cwind_cum (0.0),
    num_cwind_samples (0),
    num_qed (0),
    inflight (0),
    idle_timer (NULL)
{
  delaycb (1, 0, wrap (this, &stp_manager::ratecb));
  reset_idle_timer ();
  st = getusec ();
  stream_rpcm = New refcounted<tcp_manager> (nrcv);
}

stp_manager::~stp_manager ()
{
  if (idle_timer)
    timecb_remove (idle_timer);
}

void
stp_manager::ratecb () {
#ifdef VERBOSE_LOG
  warnx << "sent " << nsent << " RPCs in the last second\n";
  warnx << "received " << *nrcv << " RPCs in the last second\n";
  warnx << npending << " RPCs are outstanding\n";
#endif
  // do something if nsent (+ nrcv) is too high xxx?

  delaycb (1, 0, wrap (this, &stp_manager::ratecb));
  nsent = outbytes;
  *nrcv = 0;
}

long
stp_manager::doRPC_stream (ptr<location> from, ptr<location> l,
			   const rpc_program &prog, int procno, 
			   ptr<void> in, void *out, aclnt_cb cb)
{
  return stream_rpcm->doRPC (from, l, prog, procno, in, out, cb, NULL);
}

long
stp_manager::doRPC_dead (ptr<location> l,
			 const rpc_program &prog, int procno, 
			 ptr<void> in, void *out, aclnt_cb cb)
{
#ifdef VERBOSE_LOG  
  modlogger ("stp_manager") << "dead_rpc "
			    << l->id () << " " << l->address () << "\n";
#endif /* VERBOSE_LOG */  
  ref<aclnt> c = aclnt::alloc (dgram_xprt, prog, 
			       (sockaddr *)&(l->saddr ()));
  
  c->call (procno, in, out, cb); 
  return 0;
}

bool
stp_manager::room_in_window () 
{
  return inflight < cwind*CWIND_MULT;
}

long
stp_manager::doRPC (ptr<location> from, ptr<location> l,
		    const rpc_program &prog, int procno, 
		    ptr<void> in, void *out, aclnt_cb cb,
		    cbtmo_t cb_tmo)
{
  reset_idle_timer ();
  if (!room_in_window ()) {
    RPC_delay_args *args = New RPC_delay_args (from, l, prog, procno,
					       in, out, cb, cb_tmo);
    enqueue_rpc (args);
    return 0;
  } else {

    ref<aclnt> c = aclnt::alloc (dgram_xprt, prog, 
				 (sockaddr *)&(l->saddr ()));
    rpc_state *C = New rpc_state (from, l, cb, cb_tmo, seqno, 
				  prog.progno, out);

    C->procno = procno;
   
    C->b = rpccb_chord::alloc (c, 
			       wrap (this, &stp_manager::doRPCcb, c, C),
			       wrap (this, &stp_manager::timeout, C),
			       in,
			       out,
			       procno, 
			       (sockaddr *)&(l->saddr ()));
    
    long sec, nsec;
    setup_rexmit_timer (from, l, &sec, &nsec);

    //insert into the Q of RPCs in flight
    pending.insert_tail (C);
    inflight++;
    C->sendtime = getusec ();
    C->b->send (sec, nsec);
    nsent++;
    
    return seqno++;
  }
}

bool
stp_manager::timeout (rpc_state *C)
{
  //run through the list of pending RPCs and 
  // remove any that are headed for the host
  // that just timed out from the window.
  // if the host is dead, this prevents a bunch
  // of back-to-back RPCs for that host from
  // clogging up the window
  // if this was a congestion loss, we've 
  // increased the window that we "deserve"
  rpc_state *O = pending.first;
  while (O) {
    if (O->loc->id () == C->loc->id () &&
	O != C &&
	O->in_window) {
      O->in_window = false;
      inflight--;
    }
    O = pending.next (O);
  }

  // multiple retransmissions is a good sign the node is dead
  // don't hold up the window waiting for him to time out
  if (C->rexmits > 1 && C->in_window) {
    C->in_window = false;
    inflight--;
  }

  //if there are any RPCs destined for this host, make
  // them fail right away. 
  if (C->rexmits > MAX_REXMIT) {
    O = pending.first;
    while (O) {
      if (O->loc->id () == C->loc->id () && O != C) {
	rpc_state *N = pending.next (O);
	O->b->timeout ();
	O = N;
      } else {
	O = pending.next (O);
      }
    }
  }
  
  bool cancel = false;
  if (C->cb_tmo) {
    chord_node n;
    C->loc->fill_node (n);
    cancel = (C->cb_tmo)(n, C->rexmits);
  }

  C->rexmits++;
  if (C->from->id () != C->loc->id () && C->rexmits == 1)
    update_cwind (-1);

  return cancel;
}

void
stp_manager::doRPCcb (ref<aclnt> c, rpc_state *C, clnt_stat err)
{
  dorpc_res *res = (dorpc_res *)C->out;
  
  //  warn << "RPCTIMING: " << getusec () << " out = " << (u_int)C->out << " returned\n";

  if (err) {
    nrpcfailed++;
    C->loc->set_alive (false);
    warnx << gettime () << " RPC failure: " << err
          << " destined for " << C->ID
	  << " at " << inet_ntoa (C->loc->saddr().sin_addr)
	  << " seqno " << C->seqno
	  << " out " << (u_int) C->out
	  << "\n";
  } else if (res->status == DORPC_MARSHALLERR) {
    nrpcfailed++;
    err = RPC_CANTDECODEARGS;
    warnx << gettime () << " RPC Failure: DORPC_MARSHALLERR for " << C->ID 
          << "\n";
  } else if (res->status == DORPC_UNKNOWNNODE) {
    nrpcfailed++;
    C->loc->set_alive (false);
    err = RPC_SYSTEMERROR;
    warnx << gettime () << " RPC Failure: DORPC_UNKNOWNNODE for " << C->ID 
          << "\n";
  } else if (res->status == DORPC_NOHANDLER) {
    nrpcfailed++;
    err = RPC_PROGUNAVAIL;
    warnx << gettime () << " RPC Failure: DORPC_NOHANDLER for " << C->ID
	  << "\n";
  } else {
    assert (res->status == DORPC_OK);

    count_rpc (C->loc);
    update_latency (C->from, C->loc, res->resok->send_time_echo);
  }
  
  pending.remove (C);
  (C->cb) (err);
  if (C->in_window) {
    inflight--;
  }
  update_cwind (C->seqno);
  rpc_done (C->seqno);
  delete C;
}


void
stp_manager::rpc_done (long acked_seqno)
{

  if (Q.first && room_in_window ()) {
    int qsize = (num_qed > 100) ? 100 :  num_qed;
    int next = (int)(qsize*((float)random()/(float)RAND_MAX));
    RPC_delay_args *next_arg = Q.first;
    for (int i = 0; (next_arg) && (i < next); i++)
      next_arg = Q.next (next_arg);

    //if there is an earlier RPC bound for the same destination, send that
    //this preserves our use of data-driven retransmissions
    RPC_delay_args *args = Q.first;
    while ((args != next_arg) && (args->l->id () != next_arg->l->id ()))
      args = Q.next (args);

    //stats
    u_int64_t now = getusec ();
    u_int64_t diff = now - args->now;
    lat_inq.push_back (diff);
    if (lat_inq.size () > 1000) lat_inq.pop_back ();

    assert (args);
    Q.remove (args);

    doRPC (args->from, args->l, args->prog,
	   args->procno, 
	   args->in,
	   args->out,
	   args->cb,
	   args->cb_tmo);
    delete args;
    num_qed--;
  }

  if (Q.first && room_in_window () ) {
    delaycb (0, 1000000, wrap (this, &stp_manager::rpc_done, acked_seqno));
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
stp_manager::update_cwind (int seq) 
{
  if (seq >= 0) {
    if (cwind < ssthresh) 
      cwind += 1.0; //slow start
    else
      cwind += 1.0/cwind; //AI
  } else {
    ssthresh = cwind_ewma/2; // MD
    if (ssthresh < 1.0) ssthresh = 1.0;
    //    cwind = 1.0;
    cwind = cwind / 2.0;
    if (cwind < 1.0) cwind = 1.0;
  }

  cwind_ewma = (cwind_ewma*49 + cwind)/50;
  cwind_cum += cwind;
  num_cwind_samples++;
  
  //stats
  cwind_cwind.push_back (cwind);
  cwind_time.push_back ((getusec () - st)/1000000.0);
  if (cwind_cwind.size () > 1000) cwind_cwind.pop_front ();
  if (cwind_time.size () > 1000) cwind_time.pop_front ();
  
  /*
  warn << "window " << getusec ()/1000 << " " << (int)(cwind*1000) << " " << (int)(ssthresh*1000) << "\n";
  */
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
stp_manager::setup_rexmit_timer (ptr<location> from, ptr<location> l, 
				 long *sec, long *nsec)
{
#define MIN_SAMPLES 10

  float alat;

  if (nrpc < 50) {
    alat = 1000000;
  } else if (false && l &&   //if we've got a measurement, use it
      l->nrpc () > 50) {
    //      warn << l->id () << ": using " << l->nrpc () << "measurments: " << (int)l->distance () << " + 4*" << (int)l->a_var () << "\n";
    alat = 1.5*(l->distance() + 6*l->a_var () + 5000);
  } else if (l
	     && from
	     && (l->coords ().size () > 0)
	     && (from->coords ().size () > 0)) {
    float dist = Coord::distance_f (from->coords (), l->coords ());
    alat = dist + 6.0*c_err + 10*c_var + 15000; 
    //scale it to be safe. the 8 comes from an analysis for log files
    // I also tried using the variance but average works better. 
    // With 8 we'll do about 1 percent spurious retransmits
  } else
    alat = 1000000;
  
  //statistics
  timers.push_back (alat);
  if (timers.size () > 1000) timers.pop_front ();
  
  *sec = (long)(alat / 1000000);
  *nsec = ((long)alat % 1000000) * 1000;
  
  if (*nsec < 0 || *sec < 0 || *sec > 1) {
    *sec = 1;
    *nsec = 0;
  }
}

void stp_manager::stats (const strbuf &ob)
{
  char buf[1024];

  stream_rpcm->stats (ob);

  rpc_manager::stats (ob);
  
  sprintf(buf, "  Average latency/variance: %f/%f\n", a_lat, a_var);
  ob << buf;
  sprintf(buf, "  Average cwind: %f\n", cwind_cum/num_cwind_samples);
  ob << buf;

  if (shortstats) return;
  ob << "Timer history:\n";
  for (unsigned int i = 0; i < timers.size (); i++) {
    sprintf (buf, "%f", timers[i]);
    ob << "t: " << buf << "\n";
  }

  ob << "Latencies (in q):\n";
  for (unsigned int i = 0; i < lat_inq.size (); i++) {
    ob << "lat(q): " << lat_inq[i] << "\n";
  }

  ob << "cwind over time:\n";
  for (unsigned int i = 0; i < cwind_cwind.size (); i++) {
    sprintf (buf, "%f %f", cwind_time[i], cwind_cwind[i]);
    ob << "cw: " << buf << "\n";
  }

  ob << "queue length over time:\n";
  for (unsigned int i = 0; i < qued_hist.size (); i++) {
    sprintf (buf, "%f %ld", cwind_time[i], qued_hist[i]);
    ob << "qued: " << buf << "\n";
  }

  ob << "current RPCs queued for transmission pending window: " << num_qed << "\n";
  RPC_delay_args *args = Q.first;
  //  const rpcgen_table *rtp;
  u_int64_t now = getusec ();
  while (args) {
    void *args_as_pointer = args->in.get ();

    int real_prog = ((dorpc_arg *)args_as_pointer)->progno;
    int real_procno = ((dorpc_arg *)args_as_pointer)->procno;
    long diff = now - args->now;

    ob << "   " << real_prog << "." << real_procno << " for " << args->l->id() << " queued for " << diff << "\n";

    /*    rtp = &(args->prog.tbl[args->procno]);
    if (rtp) 
      warn << "  " << args->prog.name << "." << rtp->name << " for " << args->l->id() << " queued for " << diff << "\n";
    else 
      warn << "stp_manager::stats: WTF " << (u_int)&args->prog << "@" << args->procno << "\n";
    */
    args = Q.next (args);
  }

  ob << "per program bytes\n";
  rpcstats *s = rpc_stats_tab.first ();
  while (s) {
    ob << "  " << s->key << "\n";
    ob << "    calls (bytes/num):   " << s->call_bytes
       << "/" << s->ncall << "\n";
    ob << "    rexmits (bytes/num): " << s->rexmit_bytes
       << "/" << s->nrexmit << "\n";
    ob << "    replies (bytes/num): " << s->reply_bytes
       << "/" << s->nreply << "\n";
    s = rpc_stats_tab.next (s);
  }
}

// ------------- rpccb_chord ----------------

rpccb_chord *
rpccb_chord::alloc (ptr<aclnt> c,
		    aclnt_cb cb,
		    callback<bool>::ptr u_tmo,
		    ptr<void> in,
		    void *out,
		    int procno,
		    struct sockaddr *dest) {

  xdrsuio x (XDR_ENCODE);
  const rpc_program &prog = c->rp;
  
  //re-write the timestamp
  dorpc_arg *args = (dorpc_arg *)in.get ();
  args->send_time = getusec ();

  if (!aclnt::marshal_call (x, authnone_create (), prog.progno, 
			    prog.versno, procno, 
			    prog.tbl[procno].xdr_arg,
			    in)) {
    warn << "marshalling failed\n";
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
  suio *s = x.uio ();
  track_call (prog, procno, s->resid ());
  outbytes += s->resid ();
  
  ptr<bool> deleted  = New refcounted<bool> (false);

  rpccb_chord *ret = New rpccb_chord (c,
				      x,
				      cb,
				      u_tmo,
				      out,
				      prog.tbl[procno].xdr_res,
				      dest,
				      deleted,
				      in,
				      procno);
  
  return ret;
}

void
rpccb_chord::send (long _sec, long _nsec) 
{
  rexmits = 0;
  sec = _sec;
  nsec = _nsec;

  if (nsec < 0 || sec < 0)
    panic ("[send to chord-dev@amsterdam.lcs.mit.edu]: sec %ld, nsec %ld\n", sec, nsec);

  tmo = delaycb (sec, nsec, wrap (this, &rpccb_chord::timeout_cb, deleted));
  //  warn ("%s xmited %d:%06d\n", gettime().cstr(), int (sec), int (nsec/1000));

  //  warn << "RPCTIMING: " << getusec () << " sent " << (u_int)outmem << "\n";

  xmit (0);
}


//do the exponential backoff on tmo
void
rpccb_chord::reset_tmo ()
{
#ifdef VERBOSE_LOG
  long oldsec = sec;
  long oldnsec = nsec;
#endif /* VERBOSE_LOG */
  nsec *= 2;
  sec *= 2;
  while (nsec >= 1000000000) {
    nsec -= 1000000000;
    sec += 1;
  }

  if (sec > 2) sec = 2;
#ifdef VERBOSE_LOG
  warn << inet_ntoa (((sockaddr_in *)&s)->sin_addr)
       << ": timer was " << oldsec << "." << oldnsec
       << " now is " << sec << "." << nsec
       << "; rexmits is " << rexmits << "\n";
#endif /* VERBOSE_LOG */  
  if (tmo) timecb_remove (tmo);
  tmo = delaycb (sec, nsec, wrap (this, &rpccb_chord::timeout_cb, deleted));
}

void
rpccb_chord::timeout_cb (ptr<bool> del)
{
  if (*del) return;

  tmo = NULL;
  bool cancel = false;
  if (utmo)
    cancel = utmo ();

  if (rexmits > MAX_REXMIT || cancel) {
    timeout ();
    return;
  } else {
    if (nsec < 0 || sec < 0)
      panic ("1 timeout_cb: sec %ld, nsec %ld\n", sec, nsec);

    sockaddr_in *s = (sockaddr_in *)dest;
    dorpc_arg *args = (dorpc_arg *)in.get ();

    warnx << gettime () << " REXMIT " << strbuf ("%x", xid)
	  << " " << args->progno << ":" << args->procno
	  << " rexmits " << rexmits << ", timeout " 
	  << sec*1000 + nsec/(1000*1000) << " ms, destined for " 
	  << inet_ntoa (s->sin_addr) << " out is " << (u_int)outmem << "\n";

    //re-write the timestamp
    args->send_time = getusec ();

    //remarshall the args
    xdrsuio x (XDR_ENCODE);
    const rpc_program &prog = c->rp;
    if (!aclnt::marshal_call (x, authnone_create (), prog.progno, 
			      prog.versno, procno, 
			      prog.tbl[procno].xdr_arg,
			      in)) {
      fatal << "error remarshalling\n";
    }
    track_rexmit (prog, procno, x.uio()->resid ());

    //keep our old xid 
    assert (x.iov ()[0].iov_len >= 4);
    u_int32_t &txid = *reinterpret_cast<u_int32_t *> (x.iov ()[0].iov_base);
    txid = xid;

    //update the msg buffer so we send with the new timestamp
    unsigned int l = x.uio ()->resid ();
    assert (l == msglen);
    x.uio ()->copyout (msgbuf);

    //send it
    xmit (rexmits);
    
    
    if (rexmits == MAX_REXMIT && sec < MIN_RPC_FAILURE_TIMER) {
      sec = MIN_RPC_FAILURE_TIMER;
      nsec = 0;
    } 
    reset_tmo ();
    rexmits++;
  }
}

void
rpccb_chord::finish_cb (aclnt_cb cb, ptr<bool> del, clnt_stat err) 
{
  if (*del) return;
  *del = true;
  cb (err);
}

