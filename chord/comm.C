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

void
locationtable::doForeignRPC_cb (frpc_state *C, rpc_program prog,
				clnt_stat err)
{
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

void
locationtable::doRPC (chordID &ID, rpc_program prog, int procno, 
		      ptr<void> in, void *out, aclnt_cb cb)

{
  location *l = getlocation (ID);
  assert (l);
  assert (l->refcnt >= 0);
  touch_cachedlocs (l);

  doRPC_cbstate *st = New doRPC_cbstate (prog, procno, in, out, cb, ID);
  aclntudp_create (l->inetaddr,
		   l->addr.port,
		   chord_program_1,
		   wrap (this, &locationtable::doRPC_gotaxprt, st, prog));
};

void
locationtable::doRPC_gotaxprt (doRPC_cbstate *st, 
			       rpc_program prog,
			       ptr<aclnt> c,
			       clnt_stat err)
{
  
  if ( (err) || (c == NULL)) {
    chordnode->deletefingers (st->ID);
    (st->cb) (RPC_CANTSEND);
    return;
  }
  
  if (prog.progno == CHORD_PROGRAM) {
    u_int64_t s = getusec ();
    c->timedcall (60, st->procno, st->in, st->out, 
	     wrap (mkref (this), &locationtable::doRPCcb,
		   st, s));
  } else { 
    xdrproc_t inproc = prog.tbl[st->procno].xdr_arg;
    if (!inproc) return;
    
    xdrsuio x (XDR_ENCODE);
    if (!inproc (x.xdrp (), st->in)) {
      return;
    }
    
    size_t marshalled_len = x.uio ()->resid ();
    char *marshalled_data = suio_flatten (x.uio ());
    
    chord_RPC_arg farg;
    farg.v.n = st->ID;
    farg.host_prog = prog.progno;
    farg.host_proc = st->procno;
    farg.marshalled_args.setsize (marshalled_len);
    memcpy (farg.marshalled_args.base (), marshalled_data, marshalled_len);
    delete marshalled_data;
    
    location *l = getlocation (st->ID);
    assert (l);
    
    chord_RPC_res *res = New chord_RPC_res ();
    frpc_state *C = New frpc_state (res, st->out, st->procno, st->cb,
				    l, getusec ());
    c->timedcall (60, CHORDPROC_HOSTRPC, &farg, res, 
	     wrap (mkref(this), &locationtable::doForeignRPC_cb, C, prog)); 
    delete st;
  }


}


void
locationtable::doRPCcb (doRPC_cbstate *st, u_int64_t s, clnt_stat err)
{
  
  if (err) {
    nrpcfailed++;
    chordnode->deletefingers (st->ID);
  } else {
    location *l = getlocation (st->ID);
    u_int64_t lat = getusec () - s;
    l->rpcdelay += lat;
    l->nrpc++;
    rpcdelay += lat;
    nrpc++;
    if (lat > l->maxdelay) l->maxdelay = lat;
  }
  (st->cb) (err);
  delete st;
}

void
locationtable::stats () 
{
  char buf[1024];

  warnx << "LOCATION TABLE STATS: estimate # nodes " << nnodes << "\n";
  warnx << "total # of RPCs: good " << nrpc << " failed " << nrpcfailed << "\n";
    fprintf(stderr, "       Average latency: %f\n", ((float) (rpcdelay/nrpc)));
  warnx << "total # of connections opened: " << nconnections << "\n";
  warnx << "total # of connections delayed: " << ndelayedconnections << "\n";
  warnx << "total # of active connections: " << size_connections << "\n";
  warnx << "  Per link avg. RPC latencies\n";
  for (location *l = locs.first (); l ; l = locs.next (l)) {
    warnx << "    link " << l->n << " : # RPCs: " << l->nrpc << "\n";
    sprintf (buf, "       Average latency: %f\n", 
	     ((float)(l->rpcdelay))/l->nrpc);
    warnx << buf;
    sprintf (buf, "       Max latency: %qd\n", l->maxdelay);
    warnx << buf;
  }
}
