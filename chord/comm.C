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
  farg.v.n = ID;
  farg.host_prog = prog.progno;
  farg.host_proc = procno;
  farg.marshalled_args.setsize (marshalled_len);
  memcpy (farg.marshalled_args.base (), marshalled_data, marshalled_len);
  delete marshalled_data;

  location *l = getlocation (ID);
  assert (l);
  ptr<aclnt> c = aclnt::alloc (l->x, chord_program_1);
  if (c) {
    chord_RPC_res *res = New chord_RPC_res ();
    frpc_state *C = New frpc_state (res, out, prog, procno, cb,
				   l, getusec ());
    touch_connections (l);
    c->call (CHORDPROC_HOSTRPC, &farg, res, 
	   wrap (this, &locationtable::doForeignRPC_cb, C)); 
  } else {
    (*cb) (RPC_CANTSEND);
    delete_connections (l);
    chordnode->deletefingers (ID);
  }
}

void
locationtable::doForeignRPC_cb (frpc_state *C,  clnt_stat err)
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
    xdrproc_t outproc = C->prog.tbl[C->procno].xdr_res;
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
  if (l->x) {    
    assert (present_connections (l));
    if (prog.progno == CHORD_PROGRAM) {
      ptr<aclnt> c = aclnt::alloc(l->x, prog);
      if (c == 0) {
	chordnode->deletefingers (ID);
	(*cb) (RPC_CANTSEND);
	delete_connections (l);
      } else {
	u_int64_t s = getusec ();
	touch_connections (l);
	c->call (procno, in, out, wrap (mkref (this), &locationtable::doRPCcb,
				      cb, l, s));
      }
    } else doForeignRPC (prog, procno, in, out, ID, cb);
    
  } else {
    doRPC_cbstate *st = New doRPC_cbstate (prog, procno, in, out, cb, ID);
    if (l->connectlist.first) {
      l->connectlist.insert_tail (st);
    } else {
      l->connectlist.insert_tail (st);
      if (size_connections >= max_connections) {
        delay_connections (l);
      } else {
	chord_connect(ID, wrap (mkref (this), 
				&locationtable::dorpc_connect_cb, l));
      }
    }
  }
}


void
locationtable::doRPCcb (aclnt_cb cb, location *l, u_int64_t s, clnt_stat err)
{
  if (err) {
    nrpcfailed++;
    chordnode->deletefingers (l->n);
  } else {
    u_int64_t lat = getusec () - s;
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
  if (x == NULL) {
    warnx << "connect_cb: connect failed\n";
    doRPC_cbstate *st, *st1;
    for (st = l->connectlist.first; st; st = st1) {
      st1 = l->connectlist.next (st);
      chordnode->deletefingers (st->ID);
      l->connectlist.remove(st);
      aclnt_cb cb = st->cb;
      (*cb) (RPC_FAILED);
      delete st;
    }
    //    decrefcnt (l);
    return;
  }

  assert (l->refcnt >= 0);
  l->x = x;
  nconnections++;
  doRPC_cbstate *st, *st1;
  for (st = l->connectlist.first; st; st = st1) {
    if (st->progno.progno == CHORD_PROGRAM) {
      ptr<aclnt> c = aclnt::alloc (x, st->progno);
      assert (c);
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
  add_connections (l);
  //  increfcnt (ID);
  ptr<struct timeval> start = new refcounted<struct timeval>();
  gettimeofday(start, NULL);
  if (l->x) {
    (*cb)(l->x);
  } else {
    warnx << "tcpconnect: " << l->n << "@" << l->addr.hostname << " " 
	  << l->addr.port << "\n";
    tcpconnect (l->addr.hostname, l->addr.port, 
		wrap (mkref (this), &locationtable::connect_cb, l, cb));
  }
}

void
locationtable::connect_cb (location *l, 
			   callback<void, ptr<axprt_stream> >::ref cb, int fd)
{
  if (fd < 0) {
    assert (l);
    assert (l->x == 0);
    warnx << "connect failed: " << l->n << "@" << l->addr.hostname << " " 
	    << l->addr.port << "\n";
    warn (" connect failed %m\n");
    remove_connections (l);
    (*cb)(NULL);
  } else {
    tcp_nodelay(fd);
    ptr<axprt_stream> x = axprt_stream::alloc(fd);
    (*cb)(x);
  }
}

bool
locationtable::present_connections (location *l)
{
  for (location *m = connections.first; m != NULL; m = connections.next (m)) {
    if (l->n == m->n) return 1;
  }
  return 0;
}

void
locationtable::add_connections (location *l)
{
  assert (l->x == 0);
  if (size_connections >= max_connections) {
    delete_connections (connections.first);
  }
  if (present_connections (l)) {
    warnx << "add_connections: " << l->n << " is already present\n";
    assert (0);
  }
  connections.insert_tail (l);
  size_connections++;
}

void
locationtable::touch_connections (location *l)
{
  assert (l->x);
  if (!present_connections (l)) {
    warnx << "touch_connections: " << l->n << " not on connect list\n";
    assert (0);
  }
  connections.remove (l);
  connections.insert_tail (l);
}

void
locationtable::delete_connections (location *l)
{
  assert (l);
  if (l->x) {
    warnx << "delete connection to " << l->n << "@" << l->addr.hostname << " "
      	  << l->addr.port << "\n";
    if (!present_connections (l)) {
      warnx << "touch_connections: " << l->n << " not on connect list\n";
      assert (0);
    }
    connections.remove (l);
    size_connections--;
    l->x = NULL;
  }
}

void
locationtable::remove_connections (location *l)
{
  if (!present_connections (l)) {
    warnx << "touch_connections: " << l->n << " not on connect list\n";
    assert (0);
  }
  connections.remove (l);
  size_connections--;
}

void
locationtable::delay_connections (location *n)
{
  if (present_connections (n)) {
    warnx << "delay_connections: already on the connections list\n";
    assert (0);
  }
  ndelayedconnections++;
  assert (n->x == 0);
  for (location *l = delayedconnections.first; l != 0; 
       l = delayedconnections.next (l)) {
    if (l->n == n->n) return;
  }
  warnx << "delay_connection: to " << n->n << "\n";
  delayedconnections.insert_tail (n);
}

void
locationtable::cleanup_connections ()
{
  if (delayedconnections.first) {
    warnx << "cleanup_connections\n";
    while (size_connections >= max_connections / 2) {
      delete_connections (connections.first);
      location *l = delayedconnections.first;
      if (l == 0) break;
      else {
	delayedconnections.remove (l);
	chord_connect(l->n, wrap (mkref (this), 
			      &locationtable::dorpc_connect_cb, l));
      }
    }
  }
  delayed_tmo = delaycb (delayed_timer, 0, 
			 wrap (this, &locationtable::cleanup_connections));
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
