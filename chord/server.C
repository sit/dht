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

#include "chord_impl.h"

#include "stabilize.h"
#include "toe_table.h"
#include "succ_list.h"
#include "pred_list.h"

#include <coord.h>
#include "comm.h"
#include <location.h>
#include <locationtable.h>
#include <math.h>
#include <configurator.h>

#include <modlogger.h>

#define warning modlogger ("vnode", modlogger::WARNING)
#define info  modlogger ("vnode", modlogger::INFO)
#define trace modlogger ("vnode", modlogger::TRACE)

void 
vnode_impl::get_successor (ptr<location> n, cbchordID_t cb)
{
  //  warn << "get successor of " << n << "\n";
  ngetsuccessor++;
  chord_noderes *res = New chord_noderes (CHORD_OK);
  ptr<chordID> v = New refcounted<chordID> (n->id ());
  doRPC (n, chord_program_1, CHORDPROC_GETSUCCESSOR, v, res,
	 wrap (mkref (this), &vnode_impl::get_successor_cb, n->id (), cb, res));
}

void
vnode_impl::get_successor_cb (chordID n, cbchordID_t cb, chord_noderes *res, 
			      clnt_stat err) 
{
  if (err) {
    chord_node bad;
    //    warnx << "get_successor_cb: RPC failure " << err << "\n";
    cb (bad, CHORD_RPCFAILURE);
  } else if (res->status) {
    chord_node bad;
    // warnx << "get_successor_cb: RPC error " << res->status << "\n";
    cb (bad, res->status);
  } else {
    cb (make_chord_node (*res->resok), CHORD_OK);
  }
  delete res;
}

void
vnode_impl::get_succlist (ptr<location> n, cbchordIDlist_t cb)
{
  ngetsucclist++;
  chord_nodelistres *res = New chord_nodelistres (CHORD_OK);
  ptr<chordID> v = New refcounted<chordID> (n->id ());
  doRPC (n, chord_program_1, CHORDPROC_GETSUCCLIST, v, res,
	 wrap (mkref (this), &vnode_impl::get_succlist_cb, cb, res));
}

void
vnode_impl::get_succlist_cb (cbchordIDlist_t cb, chord_nodelistres *res,
			     clnt_stat err)
{
  vec<chord_node> nlist;
  if (err) {
    cb (nlist, CHORD_RPCFAILURE);
  } else if (res->status) {
    cb (nlist, res->status);
  } else {
    // xxx there must be something more intelligent to do here
    for (unsigned int i = 0; i < res->resok->nlist.size (); i++)
      nlist.push_back (make_chord_node (res->resok->nlist[i]));
    cb (nlist, CHORD_OK);
  }
  delete res;
}

void 
vnode_impl::get_predecessor (ptr<location> n, cbchordID_t cb)
{
  ptr<chordID> v = New refcounted<chordID> (n->id ());
  ngetpredecessor++;
  chord_noderes *res = New chord_noderes (CHORD_OK);
  doRPC (n, chord_program_1, CHORDPROC_GETPREDECESSOR, v, res,
	 wrap (mkref (this), &vnode_impl::get_predecessor_cb, n->id (), cb, res));
}

void
vnode_impl::get_predecessor_cb (chordID n, cbchordID_t cb, chord_noderes *res, 
				clnt_stat err) 
{
  if (err) {
    chord_node bad;
    cb (bad, CHORD_RPCFAILURE);
  } else if (res->status) {
    chord_node bad;
    cb (bad, res->status);
  } else {
    cb (make_chord_node (*res->resok), CHORD_OK);
  }
  delete res;
}


void
vnode_impl::find_succlist (const chordID &x, u_long m, cbroute_t cb,
			   ptr<chordID> guess)
{
  route_iterator *ri = produce_iterator_ptr (x);
  ri->first_hop (wrap (this, &vnode_impl::find_succlist_hop_cb, cb, ri, m),
		 guess);
}

void
vnode_impl::find_succlist_hop_cb (cbroute_t cb, route_iterator *ri, u_long m,
				  bool done)
{
  static bool shave = true;
  static bool initialized = false;
  if (!initialized) {
    int x = 1;
    assert (Configurator::only ().get_int ("chord.find_succlist_shaving", x));
    shave = (x == 1);
    initialized = true;
  }

  vec<chord_node> cs = ri->successors ();
  if (done) {
    cb (cs, ri->path (), ri->status ());
    delete ri;
    return;
  }
  if (shave) {
    size_t left = 0;
    if (cs.size () < m)
      left = cs.size ();
    else
      left = cs.size () - m;
    for (size_t i = 1; i < left; i++) {
      if (betweenrightincl (cs[i-1].x, cs[i].x, ri->key ())) {
	trace << myID << ": find_succlist (" << ri->key () << "): skipping " << i << " nodes.\n";
	cs.popn_front (i);
	cb (cs, ri->path (), ri->status ());
	delete ri;
	return;
      }
    }
  }

  ri->next_hop ();
}

void
vnode_impl::find_successor (const chordID &x, cbroute_t cb)
{
  nfindsuccessor++;
  find_route (x, wrap (mkref (this), &vnode_impl::find_successor_cb, x, cb));
}

void
vnode_impl::find_successor_cb (chordID x, cbroute_t cb, vec<chord_node> s, 
			       route search_path, chordstat status)
{
  if (status != CHORD_OK) {
    warnx << "find_successor_cb: find successor of " 
	  << x << " failed: " << status << "\n";
  } else {
    nhops += search_path.size ();
    if (search_path.size () > nmaxhops)
      nmaxhops = search_path.size ();
  }
  cb (s, search_path, status);
}

void
vnode_impl::find_route (const chordID &x, cbroute_t cb) 
{
  route_iterator *ri = produce_iterator_ptr (x);
  ri->first_hop(wrap (this, &vnode_impl::find_route_hop_cb, cb, ri), NULL);
}


void
vnode_impl::find_route_hop_cb (cbroute_t cb, route_iterator *ri, bool done)
{
  if (done) {
    cb (ri->successors (), ri->path (), ri->status ());
    delete ri;
  } else {
    ri->next_hop ();
  }
}

void
vnode_impl::notify (ptr<location> n, chordID &x)
{
  ptr<chord_nodearg> na = New refcounted<chord_nodearg>;
  chordstat *res = New chordstat;
  nnotify++;
  // warnx << gettime () << ": notify " << n << " about " << x << "\n";
  locations->lookup (x)->fill_node (na->n);
  
  doRPC (n, chord_program_1, CHORDPROC_NOTIFY, na, res, 
	 wrap (mkref (this), &vnode_impl::notify_cb, n->id (), res));
}

void
vnode_impl::notify_cb (chordID n, chordstat *res, clnt_stat err)
{
  if (err || *res) {
    if (err)
      warnx << "notify_cb: RPC failure " << n << " " << err << "\n";
    else
      warnx << "notify_cb: RPC error" << n << " " << *res << "\n";
  }
  delete res;
}

void
vnode_impl::alert (ptr<location> n, chordID &x)
{
  ptr<chord_nodearg> na = New refcounted<chord_nodearg>;
  chordstat *res = New chordstat;
  nalert++;
  warnx << "alert: " << x << " died; notify " << n->id () << "\n";
  locations->lookup (x)->fill_node (na->n);

  doRPC (n, chord_program_1, CHORDPROC_ALERT, na, res, 
	 wrap (mkref (this), &vnode_impl::alert_cb, res));
}

void
vnode_impl::alert_cb (chordstat *res, clnt_stat err)
{
  if (err) {
    warnx << "alert_cb: RPC failure " << err << "\n";
  } else if (*res != CHORD_OK) {
    warnx << "alert_cb: returns " << *res << "\n";
  }
  delete res;
}

void
vnode_impl::addHandler (const rpc_program &prog, cbdispatch_t cb) 
{
  dispatch_record *rec = New dispatch_record (prog.progno, cb);
  dispatch_table.insert (rec);
  chordnode->handleProgram (prog);
};

bool
vnode_impl::progHandled (int progno) 
{
  return (dispatch_table[progno] != NULL);
}

cbdispatch_t 
vnode_impl::getHandler (unsigned long prog) {
  dispatch_record *rec = dispatch_table[prog];
  assert (rec);
  return rec->cb;
}

void
vnode_impl::register_upcall (int progno, cbupcall_t cb)
{
  upcall_record *uc = New upcall_record (progno, cb);
  upcall_table.insert (uc);

}

void 
vnode_impl::fill_user_args (user_args *a)
{
  a->myID = myID;
  a->myindex = myindex;
  a->coords = me_->coords ();
}

user_args::~user_args () { 
  if (args) xdr_delete (prog->tbl[procno].xdr_arg, args);
};

void 
user_args::fill_from (chord_node *from)
{ 
  dorpc_arg *t_arg = transport_header ();
  const struct sockaddr_in *sa = (struct sockaddr_in *)sbp->getsa ();
  if (sa) {
    from->r.hostname = inet_ntoa (sa->sin_addr);
    from->r.port = t_arg->src_port;
    from->vnode_num = myindex;
  } else {
    // connected sockets don't have the addr field set in the sbp, so
    // we have to dig harder

    bool hasname = false;
    const ptr<asrv> srv = sbp->getsrv ();
    const ref<axprt> x = srv->xprt ();
    axprt_stream *xs = 0;

    // make a guess that this is a stream
    if (x->reliable && x->connected && 
	(xs = static_cast<axprt_stream *>(sbp->getsrv ()->xprt ().get ()))) {
      int fd = xs->getfd ();
      sockaddr_in sin;
      bzero (&sin, sizeof (sin));
      socklen_t sinlen = sizeof (sin);
      if (getpeername (fd, (sockaddr *) &sin, &sinlen) == 0) {
        from->r.hostname = inet_ntoa (sin.sin_addr);
        from->r.port = t_arg->src_port;
        from->vnode_num = myindex;
	hasname = true;
      }
    }
    if (!hasname)
      warn << "XXX cannot run fill_from on stream (?) connection\n";
  }
  from->x = t_arg->src_id;
}

void 
user_args::replyref (const int &res)
{
  reply ((void *)&res);
}

void
user_args::reply (void *res)
{
  //marshall result
  xdrproc_t inproc = prog->tbl[procno].xdr_res;
  xdrsuio x (XDR_ENCODE);
  if ((!inproc) || (!inproc (x.xdrp (), res))) 
    fatal << "couldn't marshall result\n";
  int res_len = x.uio ()->resid ();
  void *marshalled_res = suio_flatten (x.uio ());

#ifdef RPC_PROGRAM_STATS
  prog->outreply_num[procno] += 1;
  prog->outreply_bytes[procno] += res_len;
#endif

  //stuff into a transport wrapper
  dorpc_res *rpc_res = New dorpc_res (DORPC_OK);

  rpc_res->resok->src_id = myID;
  rpc_res->resok->send_time_echo = send_time;
  rpc_res->resok->src_vnode_num = myindex;
  rpc_res->resok->src_coords.setsize (coords.size ());
  for (unsigned int i = 0; i < coords.size (); i++)
    rpc_res->resok->src_coords[i] = (int)(coords[i]);


  rpc_res->resok->progno = prog->progno;
  rpc_res->resok->procno = procno;
  rpc_res->resok->results.setsize (res_len);
  memcpy (rpc_res->resok->results.base (), marshalled_res, res_len);
  xfree (marshalled_res);

  assert (rpc_res->status == DORPC_OK);

  //reply
  sbp->reply (rpc_res);
  delete rpc_res;
  delete this;
}

void
vnode_impl::ping (ptr<location>x, cbping_t cb)
{
  //close enough
  get_successor (x, wrap (this, &vnode_impl::ping_cb, cb));
}

void
vnode_impl::ping_cb (cbping_t cb, chord_node n, chordstat status) 
{
  cb (status);
}

void
vnode_impl::check_dead_node_cb (ptr<location> l, chordstat s)
{
  if (s != CHORD_OK) {
    unsigned i=0;
    for (i=0; i<dead_nodes.size (); i++)
    if (dead_nodes[i]->id () == l->id ())
        break;
    if (i == dead_nodes.size ())
      dead_nodes.push_back (l);
  }
  else {
    warn << l->id () << " back to life\n";
    chord_node n;
    l->fill_node (n);
    ptr<location> nl = locations->lookup (l->id ());
    if (nl)
      nl->set_alive (true);
    else
      locations->insert (n);
    // stabilize ();
    notify (my_succ (), myID);
  }
}

void
vnode_impl::check_dead_nodes ()
{
  vec<ptr<location> > sl = succs ();
  size_t sz = sl.size ();
  int nsucc;
  bool ok = Configurator::only ().get_int ("chord.nsucc", nsucc);
  assert (ok);

  while (!dead_nodes.empty ()) {
    ptr<location> l = dead_nodes.pop_front ();
    timespec ts;
    clock_gettime (CLOCK_REALTIME, &ts);
    if ((ts.tv_sec - l->dead_time ()) < 2592000 &&
	(sz < (unsigned)nsucc ||
	 (sz > 1 &&
	  between (sl[0]->id (), sl[sz-1]->id (), l->id ())))) {
      // not enough successors, or if the dead node could be a
      // successor
      ping (l, wrap (this, &vnode_impl::check_dead_node_cb, l));
    }
  }

  delaycb (60, 0, wrap (this, &vnode_impl::check_dead_nodes));
}


long
vnode_impl::doRPC (ref<location> l, const rpc_program &prog, int procno, 
		   ptr<void> in, void *out, aclnt_cb cb)
{
  //form the transport RPC
  ptr<dorpc_arg> arg = New refcounted<dorpc_arg> ();

  //header
  arg->dest_id = l->id ();
  arg->src_id = myID;
  arg->src_port = chordnode->get_port ();
  arg->src_vnode_num = myindex;
  
  vec<float> me = me_->coords ();
  assert (me.size ());
  arg->src_coords.setsize (me.size());
  for (size_t i = 0; i < me.size (); i++)
    arg->src_coords[i] = (int32_t) me[i];
  
  arg->progno = prog.progno;
  arg->procno = procno;

  rpc_pending_counts[prog.progno - 344447]++;
  
  //marshall the args ourself
  xdrproc_t inproc = prog.tbl[procno].xdr_arg;
  xdrsuio x (XDR_ENCODE);
  if ((!inproc) || (!inproc (x.xdrp (), in))) {
    fatal << "failed to marshall args\n";
    cb (RPC_CANTSEND);
    return 0;
  } else {
    int args_len = x.uio ()->resid ();
    arg->args.setsize (args_len);
    void *marshalled_args = suio_flatten (x.uio ());
    memcpy (arg->args.base (), marshalled_args, args_len);
    xfree (marshalled_args);

#ifdef RPC_PROGRAM_STATS
    prog.outcall_num[procno] += 1;
    prog.outcall_bytes[procno] += args_len;
#endif

    ref<dorpc_res> res = New refcounted<dorpc_res> (DORPC_OK);
    xdrproc_t outproc = prog.tbl[procno].xdr_res;
    return rpcm->doRPC (me_, l, transport_program_1, TRANSPORTPROC_DORPC, 
			arg, res, 
			wrap (this, &vnode_impl::doRPC_cb, 
			      l, outproc, out, cb, res));
  }
}


void
vnode_impl::doRPC_cb (ptr<location> l, xdrproc_t proc, 
		      void *out, aclnt_cb cb, 
		      ref<dorpc_res> res, clnt_stat err) 
{
  if (err) {
    if (!l->alive ()) {
      // benjie: no longer alive, put it on the dead_nodes list so
      // we can try to contact it periodically
      unsigned i=0;
      for (i=0; i<dead_nodes.size (); i++)
      if (dead_nodes[i]->id () == l->id ())
          break;
      if (i == dead_nodes.size ())
        dead_nodes.push_back (l);
    }
    cb (err);
  }
  else if (res->status != DORPC_OK)
    cb (RPC_CANTRECV);
  else {
    float distance = l->distance ();
    vec<float> u_coords;
    for (unsigned int i = 0; i < res->resok->src_coords.size (); i++) {
      float c = ((float)res->resok->src_coords[i]);
      u_coords.push_back (c);
    }

    l->set_coords (u_coords);
    update_coords (u_coords,
		   distance);

    //unmarshall the result and copy to out
    xdrmem x ((char *)res->resok->results.base (), 
	      res->resok->results.size (), XDR_DECODE);

    assert (proc);
    if (!proc (x.xdrp (), out)) {
      fatal << "failed to unmarshall result\n";
      cb (RPC_CANTSEND);
    } else 
      cb (err);
  }
}

#define MAXDIM 10
void
vnode_impl::update_coords (vec<float> uc, float ud)
{


  //  warn << myID << " --- starting update -----\n";
  vec<float> coords = me_->coords ();
  vec<float> v = uc;

  
  //init f
  vec<float> f;
  f.clear ();
  for (int i = 0; i < chord::NCOORDS; i++) 
    f.push_back (0.0);
  


  float actual = ud;
  float expect = Coord::distance_f (coords, uc);


  if (actual >= 0 && actual < 1000000) { //ignore timeouts
    // force magnitude: > 0 --> stretched
    float grad = expect - actual;
	  
    Coord::vector_sub (v, coords);
    
    float len = Coord::norm (v);
    while (len < 0.0001) {
      for (int i = 0; i < chord::NCOORDS; i++)
	v[i] = (double)(random () % 10 - 5) / 10.0;
      len = Coord::norm (v);
    }
    float unit = 1.0/sqrtf(len);
	  
    // scalar_mult(v, unit) is unit force vector
    // times grad gives the scaled force vector
    Coord::scalar_mult (v, unit*grad);
    
    // add v into the overall force vector
    Coord::vector_add (f, v);
    
    timestep -= 0.0025;
    timestep = (timestep < 0.05) ? 0.05 : timestep;

    Coord::scalar_mult(f, timestep);
    Coord::vector_add (coords, f);
  
#ifdef VIVALDI_DEBUG
    char b[1024];			       
    snprintf (b, 1024, "coord hop: %f - %f = %f ; len=%f ts=%f\n", 
	      expect, actual, grad, len, timestep);
    warn << b;
    Coord::print_vector ("coords ", coords);
    Coord::print_vector ("uc ", uc);
    warn << "----------------\n";
#endif 
    
    me_->set_coords (coords);
  } else {
    char b[32];
    snprintf (b, 32, "%f", actual);
    trace << "COORD: ignored actual of " << b << "\n";
  }
}

long
vnode_impl::doRPC (const chord_node &n, const rpc_program &prog, int procno, 
	      ptr<void> in, void *out, aclnt_cb cb)
{
  ptr<location> l = locations->lookup_or_create (n);
  return doRPC (l, prog, procno, in, out, cb);
}

void
vnode_impl::resendRPC (long seqno)
{
  rpcm->rexmit (seqno);
}
