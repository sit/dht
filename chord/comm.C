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

bool
locationtable::doForeignRPC (rpc_program prog,
			     unsigned long procno,
			     void *in, 
			     void *out,
			     chordID ID,
			     aclnt_cb cb) 
{
  xdrproc_t inproc = prog.tbl[procno].xdr_arg;
  if (!inproc) return false;

  xdrsuio x (XDR_ENCODE);
  if (!inproc (x.xdrp (), const_cast<void *> (in))) {
    return false;
  }

  size_t marshalled_len = x.uio ()->resid ();
  char *marshalled_data = suio_flatten (x.uio ());

  chord_RPC_arg farg;
  farg.dest = ID;
  farg.host_prog = prog.progno;
  farg.host_proc = procno;
  farg.marshalled_args.setsize (marshalled_len);
  memcpy (farg.marshalled_args.base (), marshalled_data, marshalled_len);
  delete marshalled_data;

  location *l = getlocation (ID);
  assert (l);
  ptr<aclnt> c = aclnt::alloc (l->x, chord_program_1);
  assert (c);
  chord_RPC_res *res = New chord_RPC_res ();
  c->call (CHORDPROC_HOSTRPC, &farg, res, 
	   wrap (this, &locationtable::doForeignRPC_cb, res, out, 
		 prog, procno, cb)); 
}

void
locationtable::doForeignRPC_cb (chord_RPC_res *res,
				void *out,
				rpc_program prog,
				int procno,
				aclnt_cb cb,
				clnt_stat err)
{
  if ((err) || (res->status)) (*cb)(err);
  else {
    char *mRes = res->resok->marshalled_res.base ();
    size_t reslen = res->resok->marshalled_res.size ();
    xdrmem x (mRes, reslen, XDR_DECODE);
    xdrproc_t outproc = prog.tbl[procno].xdr_res;
    if (! outproc (x.xdrp (), out) ) {
      cb (RPC_CANTDECODERES);
    } else {
      cb (RPC_SUCCESS);
    }
  }
  delete res;
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
  if (l->x) {    
    if (prog.progno == CHORD_PROGRAM) {
      ptr<aclnt> c = aclnt::alloc(l->x, prog);
      if (c == 0) {
	(*cb) (RPC_CANTSEND);
	delete_connections (l);
      } else {
	u_int64_t s = getnsec ();
	touch_connections (l);
	c->call (procno, in, out, wrap (mkref (this), &locationtable::doRPCcb,
				      cb, l, s));
      }
    } else doForeignRPC (prog, procno, in, out, ID, cb);
    
  } else {
    doRPC_cbstate *st = New doRPC_cbstate (prog, procno, in, out, cb, ID);
    l->connectlist.insert_tail (st);
    if (!l->connecting) {
      chord_connect(ID, wrap (mkref (this), 
			      &locationtable::dorpc_connect_cb, l));
    }
  }
}

void
locationtable::doRPCcb (aclnt_cb cb, location *l, u_int64_t s, clnt_stat err)
{
  if (err) {
    nrpcfailed++;
  } else {
    u_int64_t lat = getnsec () - s;
    l->rpcdelay += lat;
    l->nrpc++;
    rpcdelay += lat;
    nrpc++;
    if (lat > l->maxdelay) l->maxdelay = lat;
  }
  //  l->x = 0;
  (*cb) (err);
}

void
locationtable::dorpc_connect_cb(location *l, ptr<axprt_stream> x) 
{
  assert(l);
  l->connecting = false;
  if (x == NULL) {
    warnx << "connect_cb: connect failed\n";
    doRPC_cbstate *st, *st1;
    for (st = l->connectlist.first; st; st = st1) {
      st1 = l->connectlist.next (st);
      aclnt_cb cb = st->cb;
      (*cb) (RPC_FAILED);
      l->connectlist.remove(st);
      delete st;
    }
    //    decrefcnt (l);
    return;
  }

  assert (l->refcnt >= 0);
  l->x = x;
  l->connecting = false;
  add_connections (l);
  nconnections++;
  //  l->timeout_cb = delaycb (360, 0, wrap(this, &locationtable::timeout, l));
  doRPC_cbstate *st, *st1;
  for (st = l->connectlist.first; st; st = st1) {
    if (st->progno.progno == CHORD_PROGRAM) {
      ptr<aclnt> c = aclnt::alloc (x, st->progno);
      c->call (st->procno, st->in, st->out, st->cb);
    } else {
      doForeignRPC (st->progno, st->procno, st->in, st->out, st->ID, st->cb);
    }
    st1 = l->connectlist.next (st);
    l->connectlist.remove(st);
    delete st;
  }
}

void
locationtable::chord_connect(chordID ID, callback<void, 
			     ptr<axprt_stream> >::ref cb) 
{
  location *l = getlocation(ID);
  assert (l);
  assert (l->refcnt >= 0);
  l->connecting = true;
  //  increfcnt (ID);
  ptr<struct timeval> start = new refcounted<struct timeval>();
  gettimeofday(start, NULL);
  if (l->x) {    
    (*cb)(l->x);
  } else {
    warnx << "tcpconnect: " << l->addr.hostname << " " << l->addr.port << "\n";
    tcpconnect (l->addr.hostname, l->addr.port, 
		wrap (mkref (this), &locationtable::connect_cb, cb));
  }
}

void
locationtable::connect_cb (callback<void, ptr<axprt_stream> >::ref cb, int fd)
{
  if (fd < 0) {
    warn ("connect failed: %m\n");
    (*cb)(NULL);
  } else {
    tcp_nodelay(fd);
    ptr<axprt_stream> x = axprt_stream::alloc(fd);
    (*cb)(x);
  }
}

void
locationtable::add_connections (location *l)
{
  assert (l->x);
  if (size_connections >= max_connections) {
    delete_connections (connections.first);
  }
  connections.insert_tail (l);
  size_connections++;
}

void
locationtable::touch_connections (location *l)
{
  assert (l->x);
  connections.remove (l);
  connections.insert_tail (l);
}

void
locationtable::delete_connections (location *l)
{
  assert (l);
  assert (!l->connecting);
  if (l->x) {
    warnx << "delete_connections: delete stream to " << l->n << "\n";
    connections.remove (l);
    size_connections--;
    l->x = NULL;
  }
}

void
locationtable::stats () 
{
  char buf[1024];

  warnx << "LOCATION TABLE STATS: estimate # nodes " << nnodes << "\n";
  warnx << "total # of RPCs: good " << nrpc << " failed " << nrpcfailed << "\n";
    fprintf(stderr, "       Average latency: %f\n", ((float) (rpcdelay/nrpc)));
  warnx << "total # of connections opened: " << nconnections << "\n";
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
