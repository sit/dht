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
locationtable::timeout(location *l) {
  assert(l);
  warn << "timeout on " << l->n << " closing socket\n";
  if (l->nout == 0) l->x = NULL;
  else {
      warn << "timeout on node " << l->n << " has overdue RPCs\n";
      l->timeout_cb = delaycb(360, 0, wrap(mkref(this), 
					   &locationtable::timeout, l));
  }
}

void
locationtable::doRPC (chordID &ID, rpc_program progno, int procno, 
		      ptr<void> in, void *out, aclnt_cb cb)

{
  location *l = getlocation(ID);
  assert (l);
  assert (l->refcnt >= 0);
  l->nout++;
  if (l->x) {    
    timecb_remove(l->timeout_cb);
    l->timeout_cb = delaycb(360,0,wrap(this, &locationtable::timeout, l));
    ptr<aclnt> c = aclnt::alloc(l->x, progno);
    u_int64_t s = getnsec ();
    c->call (procno, in, out, wrap (mkref (this), &locationtable::doRPCcb,
				    cb, l, s));
  } else {
    doRPC_cbstate *st = New doRPC_cbstate (progno, procno, in, out,  cb);
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
    }
    decrefcnt (l);
    return;
  }

  assert (l->refcnt >= 0);
  l->x = x;
  l->connecting = false;
  l->timeout_cb = delaycb (360, 0, wrap(this, &locationtable::timeout, l));
  doRPC_cbstate *st, *st1;
  for (st = l->connectlist.first; st; st = st1) {
    ptr<aclnt> c = aclnt::alloc (x, st->progno);
    c->call (st->procno, st->in, st->out, st->cb);
    st1 = l->connectlist.next (st);
    l->connectlist.remove(st);
  }
  decrefcnt (l);
}

void
locationtable::chord_connect(chordID ID, callback<void, 
			     ptr<axprt_stream> >::ref cb) 
{
  location *l = getlocation(ID);
  assert (l);
  assert (l->refcnt >= 0);
  l->connecting = true;
  increfcnt (ID);
  ptr<struct timeval> start = new refcounted<struct timeval>();
  gettimeofday(start, NULL);
  if (l->x) {    
    timecb_remove(l->timeout_cb);
    l->timeout_cb = delaycb(360, 0, wrap(mkref(this), 
					 &locationtable::timeout, l));
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
locationtable::stats () 
{
  warnx << "LOCATION TABLE STATS\n";
  warnx << "total # of RPCs: good " << nrpc << " failed " << nrpcfailed << "\n";
    fprintf(stderr, "       Average latency: %fn\n", ((float) (rpcdelay/nrpc)));
  warnx << "  Per link avg. RPC latencies\n";
  for (location *l = locs.first (); l ; l = locs.next (l)) {
    warnx << "    link " << l->n << "\n";
    fprintf(stderr, "       Average latency: %f\n", 
	    ((float)(l->rpcdelay))/l->nrpc);
    fprintf (stderr, "       Max latency: %qd\n", l->maxdelay);
  }
}
