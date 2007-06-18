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

const int aclnttrace (getenv ("ACLNT_TRACE")
		      ? atoi (getenv ("ACLNT_TRACE")) : 0);
const bool aclnttime (getenv ("ACLNT_TIME"));

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
vnode_impl::get_predlist (ptr<location> n, cbchordIDlist_t cb)
{
  ngetsucclist++;
  chord_nodelistres *res = New chord_nodelistres (CHORD_OK);
  ptr<chordID> v = New refcounted<chordID> (n->id ());
  //we can use the same callback
  doRPC (n, chord_program_1, CHORDPROC_GETPREDLIST, v, res,
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
    for (unsigned int i = 0; i < res->resok->nlist.size (); i++) {
      chord_node n = make_chord_node (res->resok->nlist[i]);
      nlist.push_back (n);
      //update the coordinates if this node is in the location table
      ptr<location> l = locations->lookup (n.x);
      if (l) {
	l->set_coords (n);
      }
    }
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
    else {
      warnx << "notify_cb: RPC error " << n << " " << *res << "\n";
      ptr<location> nl = locations->lookup (n);
      nl->set_alive (false);
    }
  }
  delete res;
}

void
vnode_impl::alert (ptr<location> n, ptr<location> x)
{
  ptr<chord_nodearg> na = New refcounted<chord_nodearg>;
  chordstat *res = New chordstat;
  nalert++;
  warnx << "alert: " << x->id() << " died; notify " << n->id () << "\n";
  x->fill_node (na->n);

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
  a->me_ = me_;
}

user_args::~user_args () { 
  if (args) xdr_delete (prog->tbl[procno].xdr_arg, args);
}

void 
user_args::fill_from (chord_node *from)
{ 
  dorpc_arg *t_arg = transport_header ();
  *from = make_chord_node (t_arg->src);
#if 0
  /**** I don't think this is needed any more ****/
  const struct sockaddr_in *sa = (struct sockaddr_in *)sbp->getsa ();
  if (sa) {
    from->r.hostname = inet_ntoa (sa->sin_addr);
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
	hasname = true;
      }
    }
    if (!hasname)
      warn << "XXX cannot run fill_from on stream (?) connection\n";
  }
#endif /* 0 */
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
  track_reply (*prog, procno, res_len);

  //stuff into a transport wrapper
  dorpc_res *rpc_res = New dorpc_res (DORPC_OK);
  
  me_->fill_node (rpc_res->resok->src);
  rpc_res->resok->send_time_echo = send_time;
  rpc_res->resok->results.setsize (res_len);
  if (res_len > 0)
    x.uio ()->copyout (rpc_res->resok->results.base ());

  assert (rpc_res->status == DORPC_OK);
  
  u_int64_t diff = getusec (true) - init_time;
  track_proctime (*prog, procno, diff);
 
  //reply
  sbp->reply (rpc_res);
  delete rpc_res;
  delete this;
}

void
vnode_impl::ping (ptr<location> x, cbping_t cb)
{
  ptr<chordID> v = New refcounted<chordID> (x->id ());
  ptr<dorpc_arg> arg = marshal_doRPC (x, chord_program_1,
      CHORDPROC_NULL, v);
  ref<dorpc_res> res = New refcounted<dorpc_res> (DORPC_OK);
  //talk directly to the RPC manger to get the dead behaviour
  rpcm->doRPC_dead (x, transport_program_1, TRANSPORTPROC_DORPC, 
		    arg, res, 
		    wrap (this, &vnode_impl::ping_cb, x, res, cb));
}

void
vnode_impl::ping_cb (ptr<location> x, ptr<dorpc_res> res,
    cbping_t cb, clnt_stat status) 
{
  if (status || res->status) {
    x->set_alive (false);
    cb (CHORD_RPCFAILURE);
  } else {
    x->set_alive (true);
    cb (CHORD_OK);
  }
}

void
vnode_impl::check_dead_node_cb (ptr<location> l, time_t nbackoff, chordstat s)
{
  unsigned int i=0;
  chordID id = l->id ();
  for (i=0; i<dead_nodes.size (); i++)
    if (dead_nodes[i]->id () == id)
      break;

  if (s != CHORD_OK) {
    if (i == dead_nodes.size ())
      dead_nodes.push_back (l);
    delaycb (nbackoff, wrap (this, &vnode_impl::check_dead_node, l, nbackoff));
  }
  else {
    /* Take it off the dead list */
    if (i != dead_nodes.size ()) {
      dead_nodes[i] = dead_nodes[0];
      dead_nodes.pop_front ();
    }

    // Insertion should ensure that node is set to alive and in table
    // (Liveness should also have been set by ping_cb)
    ptr<location> nl = locations->insert (l);
    if (nl != l) {
      warn << "duplicate location  " << l << "\n";
    }
    notify (my_succ (), myID);
  }
}

void
vnode_impl::check_dead_node (ptr<location> l, time_t backoff)
{
  int cap;
  assert (Configurator::only ().get_int ("chord.checkdead_max", cap));

  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);

  if ((ts.tv_sec - l->dead_time ()) > 86400 * 30) {
    // Throw away nodes that are really really dead,
    // though they remain in the location table.
    warnx << "Node " << l << " dead for 30 days\n";
    return;
  }

  backoff *= 2;
  if (backoff > cap)
    backoff = cap;
  ping (l, wrap (this, &vnode_impl::check_dead_node_cb, l, backoff));
  warnx << gettime () << " pinging dead node " << l << "\n";
}

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

ptr<dorpc_arg> vnode_impl::marshal_doRPC (ref<location> l,
    const rpc_program &prog, int procno, ptr<void> in)
{
  //form the transport RPC
  ptr<dorpc_arg> arg = New refcounted<dorpc_arg> ();

  //header

  l->fill_node (arg->dest);
  me_->fill_node (arg->src);
  
  arg->progno = prog.progno;
  arg->procno = procno;

  //marshall the args ourself
  xdrproc_t inproc = prog.tbl[procno].xdr_arg;
  xdrsuio x (XDR_ENCODE);
  if ((!inproc) || (!inproc (x.xdrp (), in))) {
    fatal << "failed to marshall args\n";
    return NULL;
  } else {
    int args_len = x.uio ()->resid ();
    arg->args.setsize (args_len);
    if (args_len > 0)
      x.uio ()->copyout (arg->args.base ());
  }
  return arg;
}

void
err_cb (aclnt_cb cb)
{
  cb (RPC_CANTSEND);
}
long
vnode_impl::doRPC (ref<location> l, const rpc_program &prog, int procno, 
		   ptr<void> in, void *out, aclnt_cb cb,
		   cbtmo_t cb_tmo /* = NULL */, bool stream /* = false */)
{

  //check to see if this is alive
  ptr<location> loc = locations->lookup (l->id ());
  if (loc && !loc->alive()) {
    warn << my_ID () << ": doRPC (" << prog.name << "." << procno 
         << ") on dead node " << l->address () << "\n";
    delaycb (0, wrap (&err_cb, cb));
    return -1;
  }

  ptr<dorpc_arg> arg = marshal_doRPC (l, prog, procno, in);
  if (!arg) {
    cb (RPC_CANTSEND);
    return 0;
  } else {
    // This is the real call; cf transport tracking in stp_manager.
    track_call (prog, procno, arg->args.size ());

    ref<dorpc_res> res = New refcounted<dorpc_res> (DORPC_OK);
    xdrproc_t outproc = prog.tbl[procno].xdr_res;
    u_int32_t xid = random_getword ();

    // Stolen (mostly) from aclnt::init_call
    if (aclnttrace >= 2) {
      str name;
      const rpcgen_table *rtp;
      rtp = &prog.tbl[procno];
      assert (rtp);
      name = strbuf ("%s:%s fake_xid=%x", prog.name, rtp->name, xid)
	 	 << " on " << l->address () << ":" << l->vnode ();
      
      warn << "ACLNT_TRACE:" << tracetime () << " call " << name << "\n";
      if (aclnttrace >= 5 && rtp->print_arg)
	rtp->print_arg (in, NULL, aclnttrace - 4, "ARGS", "");
      if (aclnttrace >= 3 && cb != aclnt_cb_null)
	cb = wrap (printreply, cb, name, out, rtp->print_res);
    }
    aclnt_cb cbw = wrap (this, &vnode_impl::doRPC_cb, 
			l, outproc, out, cb, res);
  
    if (!stream)
      return rpcm->doRPC (me_, l, transport_program_1, TRANSPORTPROC_DORPC, 
			  arg, res, cbw, 
			  wrap(this, &vnode_impl::tmo, cb_tmo, 
			       prog.progno, procno, arg->args.size ()));
    else
      return rpcm->doRPC_stream (me_, l, 
				 transport_program_1, TRANSPORTPROC_DORPC, 
				 arg, res, cbw);
  }
}

bool
vnode_impl::tmo (cbtmo_t cb_tmo, int progno, 
		 int procno, int args_len, chord_node n, int r)
{
  bool cancel = false;
  if (cb_tmo) cancel = cb_tmo (n, r);
  // Track the rexmit for the real program;
  // rpccb_chord::timeout_cb will track for transport_prot.
  if (!cancel)
    track_rexmit (progno, procno, args_len);
  return cancel;
}

void
vnode_impl::doRPC_cb (ptr<location> l, xdrproc_t proc,
		      void *out, aclnt_cb cb,
		      ref<dorpc_res> res, clnt_stat err)
{
  if (err) {
    ptr<location> reall = locations->lookup (l->id ());
    if (reall && reall->alive ()) {
      warn << "got error " << err << ", but " << l
	   << " is still marked alive\n";
      reall->set_alive (false);
    } else if (!reall) {
      locations->insert (l);
      l->set_alive (false);
    }
    if (!l->alive () && checkdead_int > 0) {
      // benjie: no longer alive, put it on the dead_nodes list so
      // we can try to contact it periodically
      unsigned i=0;
      for (i=0; i<dead_nodes.size (); i++)
	if (dead_nodes[i]->id () == l->id ())
          break;
      if (i == dead_nodes.size ()) {
        dead_nodes.push_back (l);
	delaycb (checkdead_int, wrap (this, &vnode_impl::check_dead_node, l, checkdead_int));
      }
    }
    cb (err);
  }
  else if (res->status != DORPC_OK)
    cb (RPC_CANTRECV);
  else {
    float distance = l->distance ();

    chord_node n = make_chord_node (res->resok->src);

    l->set_coords (n);
    Coord u_coords (n);
    update_coords (u_coords, distance);

    // This should reset the age of the node to zero because
    // remote side always provides an age of zero for self
    // and locationtable will pull in updates that are younger.
    if (me_->id () != n.x)
      locations->insert (n);

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

void
vnode_impl::update_error (float actual, float expect, float rmt_err)
{
  if (actual < 0) return;
  float rel_error = fabs (expect - actual)/actual;

  float pred_err = me_->coords ().err ();
  float npe = -1.0;

  if (pred_err < 0)
    npe = rel_error;
  else if (rmt_err < 0)
    npe = pred_err;
  else {
    // ce is our pred error, he is the remote pred error
    // squaring them punishes high error nodes relatively more.
    float ce = pred_err*pred_err;
    float he = rmt_err*rmt_err;
    //this is our new prediction error found by combining our prediction
    // error with the remote node's error
    float new_pred_err = rel_error*(ce/(he + ce)) + pred_err*(he/(he + ce));

    //don't just take the new prediction, EWMA it with old predictions
    npe = (19*pred_err + new_pred_err)/20.0;

    //cap it at 1.0
    if (npe > 1.0) npe = 1.0;
  }
  me_->set_coords_err (npe);
}

#define MAXDIM 10
void
vnode_impl::update_coords (Coord uc, float ud)
{
  //  warn << myID << " --- starting update -----\n";
  Coord v = uc;
  float rmt_err = uc.err ();

  float actual = ud;
  float expect = me_->coords ().distance_f (uc);

  if (actual >= 0 && actual < 1000000) { //ignore timeouts
    //track our prediction error
    update_error (actual, expect, rmt_err);
    // Work on a copy (including the updated error).
    Coord coords = me_->coords ();

    // force magnitude: > 0 --> stretched
    float grad = expect - actual;
    v.vector_sub (coords);

    float len = v.plane_norm ();
    while (len < 0.0001) {
      for (unsigned int i = 0; i < Coord::NCOORD; i++)
	v.coords[i] = (double)(random () % 400 - 200) / 1.0;
      //if (USING_HT) v.ht += fabs((double)(random () % 10 - 5) / 10.0);
      len = v.plane_norm ();
    }
    len = v.norm ();

    float unit = 1.0 / sqrtf (len);

    // scalar_mult(v, unit) is unit force vector
    // times grad gives the scaled force vector
    v.scalar_mult (unit*grad);

    //timestep is the scaled ratio of our prediction error
    // and the remote node's prediction error
    float pred_err = coords.err ();
    float timestep;
    if (pred_err > 0 && rmt_err > 0)
      timestep = 0.1 * (pred_err)/(pred_err + rmt_err);
    else if (pred_err > 0)
      timestep = 0.0;
    else
      timestep = 1.0;

    v.scalar_mult (timestep);
    //flip sign on height
    v.ht = -v.ht;

    coords.vector_add (v);

#ifdef VIVALDI_DEBUG
    char b[1024];
    snprintf (b, 1024, "coord hop: %f - %f = %f ; len=%f ts=%f (%f %f)\n",
	      expect, actual, grad, len, timestep, pred_err, rmt_err);
    warn << b;
    coords.print ("coords ");
    uc.print ("uc ");
    warn << "----------------\n";
#endif

    if (coords.ht <= 100) coords.ht = 100;
    me_->set_coords (coords);
  } else {
    char b[32];
    snprintf (b, 32, "%f", actual);
    trace << "COORD: ignored actual of " << b << "\n";
  }
}

long
vnode_impl::doRPC (const chord_node &n, const rpc_program &prog, int procno, 
		   ptr<void> in, void *out, aclnt_cb cb, 		    
		   cbtmo_t cb_tmo, bool stream)
{
  ptr<location> l = locations->lookup_or_create (n);
  return doRPC (l, prog, procno, in, out, cb, cb_tmo, stream);
}
