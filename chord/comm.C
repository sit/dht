/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#include "chord.h"
#include "math.h"

#define TIMEOUT 10
int seqno = 0;
int st = getusec ();

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

const int fclnttrace (getenv ("FCLNT_TRACE")
		      ? atoi (getenv ("FCLNT_TRACE")) : 0);
void
locationtable::doForeignRPC_cb (frpc_state *C, rpc_program prog,
				ptr<aclnt> c, 
				clnt_stat err)
{
  npending--;
  if (C->tmo) {
    timecb_remove (C->tmo);
    C->tmo = NULL;
  }

  if ((err) || (C->res->status)) {
    nrpcfailed++;
    chordnode->deletefingers (C->ID);
    (C->cb)(err);
  } else {
    u_int64_t lat = getusec () - C->s;
    update_latency (C->ID, lat);

    char *mRes = C->res->resok->marshalled_res.base ();
    size_t reslen = C->res->resok->marshalled_res.size ();
    xdrmem x (mRes, reslen, XDR_DECODE);
    xdrproc_t outproc = prog.tbl[C->procno].xdr_res;
    if (! outproc (x.xdrp (), C->out) ) {
      C->cb (RPC_CANTDECODERES);
    } else {
      C->cb (RPC_SUCCESS);
    }
  }

  rpc_done (C);

  delete C->res;
  delete C;

}


long
locationtable::new_xid (svccb *sbp)
{
  last_xid += 1;
  octbl.insert (last_xid, sbp);
  return last_xid;
}

void
locationtable::reply (long xid, 
		      void *out,
		      long outlen) 
{

  svccb **sbp = octbl[xid];
  assert (sbp);
  chord_RPC_res res;
  res.set_status (CHORD_OK);
  res.resok->marshalled_res.setsize (outlen);
  memcpy (res.resok->marshalled_res.base (), out, outlen);
  (*sbp)->replyref (res);
  octbl.remove (xid);
  
}

#ifdef FAKE_DELAY
void
locationtable::doRPC_delayed (RPC_delay_args *args) 
{
  chordID ID = args->ID;
  doRPC (ID, ID, args->prog,
	 args->procno, 
	 args->in,
	 args->out,
	 args->cb,
	 args->s);
  delete args;
}
#endif /* FAKE_DELAY */

void
locationtable::doRPC (chordID &from, chordID &ID, 
		      rpc_program prog, int procno, 
		      ptr<void> in, void *out, aclnt_cb cb,
		      u_int64_t sp)

{

#ifdef FAKE_DELAY
  long dist = geo_distance (from, ID);
  if (dist) {
    RPC_delay_args *args = New RPC_delay_args (ID, prog, procno,
					       in, out, cb, getusec ());
    delaycb (0, dist*1000000, 
	     wrap (this, &locationtable::doRPC_delayed, args));
    return;
  }
#endif /* FAKE_DELAY */

#ifdef WINDOW_SCHEME
  reset_idle_timer ();
  if ((prog.progno != CHORD_PROGRAM) && (left + cwind < seqno)) {
    RPC_delay_args *args = New RPC_delay_args (ID, prog, procno,
					       in, out, cb, getusec ());
    enqueue_rpc (args);
    return;
  }
#endif

  sp = getusec ();

  location *l = getlocation (ID);
  assert (l);
  assert (l->refcnt >= 0);
  touch_cachedlocs (l);

  ptr<aclnt> c = aclnt::alloc (dgram_xprt, chord_program_1, 
			       (sockaddr *)&(l->saddr));
  if (c == NULL) {
    chordnode->deletefingers (ID);
    (cb) (RPC_CANTSEND);
    return;
  }

  /* statistics */
  nsent++;

  if (prog.progno == CHORD_PROGRAM) {
    c->timedcall (TIMEOUT, procno, in, out, 
		  wrap (mkref (this), &locationtable::doRPCcb,
			ID, cb, sp, c));
    
  } else { 
    xdrproc_t inproc = prog.tbl[procno].xdr_arg;
    if (!inproc) {
      (*cb)(RPC_CANTSEND);
      return;
    }
    
    xdrsuio x (XDR_ENCODE);
    if (!inproc (x.xdrp (), in)) {
      (*cb)(RPC_CANTSEND);
      return;
    }

    size_t marshalled_len = x.uio ()->resid ();
    char *marshalled_data = suio_flatten (x.uio ());

    chord_RPC_arg *farg = New chord_RPC_arg ();
    farg->v.n = ID;
    farg->host_prog = prog.progno;
    farg->host_proc = procno;
    farg->marshalled_args.setsize (marshalled_len);
    memcpy (farg->marshalled_args.base (), marshalled_data, marshalled_len);
    delete marshalled_data;
    
    if (fclnttrace > 1) {
      const rpcgen_table *rtp = &prog.tbl[procno];
      str name = strbuf ("%s:%s", prog.name, rtp->name);
      warn << "call " << name << "\n";
    }
    chord_RPC_res *res = New chord_RPC_res ();
    frpc_state *C = New frpc_state (res, out, procno, cb,
				    ID, sp, marshalled_len, seqno);
    setup_rexmit_timer (ID, C);
    
    //add to outstanding Q
    sent_Q.insert_tail (C);

    issue_RPC (seqno, c, farg, res,  wrap (mkref(this), 
					   &locationtable::doForeignRPC_cb, 
					   C, prog, c));
    seqno++;
  }

}

void
locationtable::issue_RPC (long sq, ptr<aclnt> c, chord_RPC_arg *farg, 
			  chord_RPC_res *res, aclnt_cb cb) 
{

    c->timedcall (TIMEOUT, CHORDPROC_HOSTRPC, 
		farg, res, cb); 

#if 0
  char time[128];
  sprintf (time, "%f", (getusec () - st)/1000000.0);
  warn << "sent: " << time << " " << sq << "\n";
#endif

  delete farg;
}


void
locationtable::issue_RPC_delay (long sq, ptr<aclnt> c, chord_RPC_arg *farg, 
			  chord_RPC_res *res, aclnt_cb cb) 
{

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
  idle_timer = NULL;
}

void
locationtable::update_cwind (int seq) 
{

  if (seq >= 0) {
    cwind += 1.0/cwind; //AI
    if (seq == left) {
      if (sent_Q.first) 
	left = sent_Q.first->seqno;
      else
	left = seqno;

      //      warn << "new left is " << left << "\n";
    }
  } else {
    cwind /= 2.0; //MD
    if (cwind < 1.0) cwind = 1.0; //avoid silly window
  }

#if 0
  char time[128];
  sprintf (time, "%f", (getusec () - st)/1000000.0);
  printf("cwind: %s %f\n", time, cwind);
#endif

}

void
locationtable::enqueue_rpc (RPC_delay_args *args) 
{
  Q.insert_tail (args);
}

void
locationtable::rpc_done (frpc_state *C)
{

  if (!C) {
    update_cwind (-1);
    return;
  }

#if 0
  char time[128];
  sprintf (time, "%f", (getusec () - st)/1000000.0);
  warn << "acked: " << time << " " << C->seqno << "\n";

  frpc_state *s = sent_Q.first;
  while (s) {
    warn << s->seqno << " ";
    s = sent_Q.next (s);
  }
  warn << "\n";
#endif

  sent_Q.remove (C);
  update_cwind (C->seqno);

  while (Q.first && (left + cwind >= seqno) ) {
    RPC_delay_args *args = Q.remove (Q.first);
    chordID ID = args->ID;
    doRPC (ID, ID, args->prog,
	   args->procno, 
	   args->in,
	   args->out,
	   args->cb,
	   args->s);
    delete args;    
  }

}

void
locationtable::setup_rexmit_timer (chordID ID, frpc_state *C)
{
#define MIN_SAMPLES 10
  location *l = getlocation (ID);
  long sec;
  long nsec;
  float alat;
  if (l->nrpc > MIN_SAMPLES) 
    alat = 3*l->a_lat;
  else
    alat = 3*a_lat;

  sec = (int)alat;
  nsec = (long)(alat - (int)alat) * 1000000000;

  //  C->tmo = delaycb (sec, nsec, wrap (this, &locationtable::timeout_cb, C));
  C->tmo = NULL;
}

void
locationtable::timeout_cb (frpc_state *C)
{
  rpc_done (NULL);
  C->tmo = NULL;
}

void
locationtable::rexmit_handler (long seqno)
{
  warn << "retransmit " << seqno << "\n";
  rpc_done (NULL);
}

void
locationtable::update_latency (chordID ID, u_int64_t lat)
{
  location *l = getlocation (ID);
  assert (l);

  l->rpcdelay += lat;
  l->nrpc++;
  rpcdelay += lat;
  nrpc++;

  l->a_lat = l->a_lat + 0.2*(lat - l->a_lat);
  a_lat = a_lat + 0.2*(lat - a_lat);

  if (lat > l->maxdelay) l->maxdelay = lat;
}
void
locationtable::doRPCcb (chordID ID, aclnt_cb cb,  u_int64_t s, 
			ptr<aclnt> c, clnt_stat err)
{

  if (err) {
    nrpcfailed++;
    chordnode->deletefingers (ID);
  } else {
    u_int64_t lat = getusec () - s;
    update_latency (ID, lat);
  }
  (cb) (err);
}

void
locationtable::stats () 
{
  char buf[1024];

  warnx << "LOCATION TABLE STATS: estimate # nodes " << nnodes << "\n";
  warnx << "total # of RPCs: good " << nrpc << " failed " << nrpcfailed << "\n";
  sprintf(buf, "       Average latency: %f\n", ((float) (rpcdelay/nrpc)));
  warnx << buf << "  Per link avg. RPC latencies\n";
  for (location *l = locs.first (); l ; l = locs.next (l)) {
    warnx << "    link " << l->n << " : refcnt: " << l->refcnt << " # RPCs: "
	  << l->nrpc << "\n";
    sprintf (buf, "       Average latency: %f\n", 
	     ((float)(l->rpcdelay))/l->nrpc);
    warnx << buf;
    sprintf (buf, "       Max latency: %qd\n", l->maxdelay);
    warnx << buf;
  }
}
