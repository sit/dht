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
  if ((err) || (C->res->status)) {
    nrpcfailed++;
    chordnode->deletefingers (C->l->n);
    (C->cb)(err);
  } else {
    u_int64_t lat = getusec () - C->s;
    C->l->rpcdelay += lat;
    C->l->nrpc++;
    rpcdelay += lat;
    nrpc++;
    C->l->maxdelay = (lat > C->l->maxdelay) ? lat : C->l->maxdelay;

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
#else
  sp = getusec ();
#endif /* FAKE_DELAY */

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
  npending++;

  if (prog.progno == CHORD_PROGRAM) {
    c->timedcall (TIMEOUT, procno, in, out, 
	     wrap (mkref (this), &locationtable::doRPCcb,
		   ID, cb, sp, c));
  } else { 
    xdrproc_t inproc = prog.tbl[procno].xdr_arg;
    if (!inproc) return;
    
    xdrsuio x (XDR_ENCODE);
    if (!inproc (x.xdrp (), in)) {
      return;
    }
    
    size_t marshalled_len = x.uio ()->resid ();
    char *marshalled_data = suio_flatten (x.uio ());
    
    chord_RPC_arg farg;
    farg.v.n = ID;
    farg.host_prog = prog.progno;
    farg.host_proc = procno;
    farg.marshalled_args.setsize (marshalled_len);
    memcpy (farg.marshalled_args.base (), marshalled_data, marshalled_len);
    delete marshalled_data;
    
    chord_RPC_res *res = New chord_RPC_res ();
    frpc_state *C = New frpc_state (res, out, procno, cb,
				    l, sp);
    if (fclnttrace > 1) {
      //      str name = strbuf ("prog %d proc %d",prog.progno,  procno);
      const rpcgen_table *rtp = &prog.tbl[procno];
      str name = strbuf ("%s:%s", prog.name, rtp->name);
      warn << "call " << name << "\n";
    }
    c->timedcall (TIMEOUT, CHORDPROC_HOSTRPC, &farg, res, 
	     wrap (mkref(this), &locationtable::doForeignRPC_cb, C, prog, c)); 
  }

}


void
locationtable::doRPCcb (chordID ID, aclnt_cb cb,  u_int64_t s, 
			ptr<aclnt> c, clnt_stat err)
{

  npending--;
  if (err) {
    nrpcfailed++;
    chordnode->deletefingers (ID);
  } else {
    location *l = getlocation (ID);
    assert (l);
    u_int64_t lat = getusec () - s;
    l->rpcdelay += lat;
    l->nrpc++;
    rpcdelay += lat;
    nrpc++;
    if (lat > l->maxdelay) l->maxdelay = lat;
  }
  (cb) (err);
}

void
locationtable::stats () 
{
  char buf[1024];

  warnx << "LOCATION TABLE STATS: estimate # nodes " << nnodes << "\n";
  warnx << "total # of RPCs: good " << nrpc << " failed " << nrpcfailed << "\n";
  warnx << "       RPCs in last second (send/recv): (" << nsent << "/" 
	<< chordnode->nrcv << ")\n";
  warnx <<  "       RPCs outstanding: " <<  npending << "\n";
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
