#include "recroute.h"
#include "fingerroutepns.h"
#include <coord.h>
#include <location.h>
#include <locationtable.h>
#include <misc_utils.h>
#include <modlogger.h>

#define rwarning modlogger ("recroute", modlogger::WARNING)
#define rinfo    modlogger ("recroute", modlogger::INFO)
#define rtrace   modlogger ("recroute", modlogger::TRACE)


template<class T>
ref<vnode>
recroute<T>::produce_vnode (ref<chord> _chordnode,
			    ref<rpc_manager> _rpcm,
			    ref<location> _l)
{
  return New refcounted<recroute<T> > (_chordnode, _rpcm, _l);
}

template<class T>
recroute<T>::recroute (ref<chord> _chord,
		       ref<rpc_manager> _rpcm,
		       ref<location> _l)
  : T (_chord, _rpcm, _l),
    sweep_cb (NULL)
{
  addHandler (recroute_program_1, wrap (this, &recroute<T>::dispatch));

  {
    int x = 1;
    assert (Configurator::only ().get_int ("chord.find_succlist_shaving", x));
    assert (Configurator::only ().get_int ("chord.lookup_timeout", lto));
    shave = (x == 1);
  }

  sweep_cb = delaycb (lto, 0, wrap (this, &recroute<T>::sweeper));

}

template<class T>
recroute<T>::~recroute ()
{
  if (sweep_cb) {
    timecb_remove (sweep_cb);
    sweep_cb = NULL;
  }
  route_recchord *r = routers.first ();
  route_recchord *rn = NULL;
  while (r != NULL) {
    rn = routers.next (r);
    routers.remove (r);
    delete r;
    r = rn;
  }
}

template<class T>
void
recroute<T>::sweeper ()
{
  sweep_cb = NULL;

  u_int swept = 0;
  u_int started = 0;
  u_int total = 0;

  timespec now;
  clock_gettime (CLOCK_REALTIME, &now);
  timespec maxtime;
  maxtime.tv_sec = lto;
  maxtime.tv_nsec = 0;

  route_recchord *r = routers.first ();
  route_recchord *rn = NULL;
  while (r != NULL) {
    rn = routers.next (r);
    total++;
    if (r->started ()) {
      started++;
      timespec st = r->start_time ();
      if (now - st > maxtime) {
	swept++;
	routers.remove (r);
	r->handle_timeout ();
      }
    }
    r = rn;
  }
  rtrace << this->my_ID () << ": sweeper: swept " << swept  << "/" << started
	 << " started routers (" << total <<" total).\n";
  sweep_cb = delaycb (maxtime.tv_sec, wrap (this, &recroute<T>::sweeper));
}

template<class T>
void
recroute<T>::dispatch (user_args *a)
{
  if (a->prog->progno != recroute_program_1.progno) {
    T::dispatch (a);
    return;
  }

  switch (a->procno) {
  case RECROUTEPROC_NULL:
    a->reply (NULL);
    break;
  case RECROUTEPROC_ROUTE:
    {
      // go off and do recursive next hop and pass it on.
      recroute_route_arg *ra = a->template getarg<recroute_route_arg> ();
      dorecroute (a, ra);
    }
    break;
  case RECROUTEPROC_PENULTIMATE:
    {
      recroute_penult_arg *ra = a->template getarg<recroute_penult_arg> ();
      dopenult (a, ra);
    }
    break;
  case RECROUTEPROC_COMPLETE:
    {
      // Try to see if this is one of ours.
      // If so, we'll need to pass things back up to whoever called us.
      recroute_complete_arg *ca =
	a->template getarg<recroute_complete_arg> ();
      docomplete (a, ca);
    }
    break;
  default:
    a->reject (PROC_UNAVAIL);
    break;
  }
}

template<class T>
void
recroute<T>::dorecroute (user_args *sbp, recroute_route_arg *ra)
{
  recroute_route_stat rstat (RECROUTE_ACCEPTED);

  chordID myID = this->my_ID ();
  strbuf header;

  header << myID << ": dorecroute (" << ra->routeid << ", " << ra->x << "): ";
  
  rtrace << header << "starting; desired = " << ra->succs_desired << "\n";
  
  vec<ptr<location> > cs = this->succs ();
  u_long m = ra->succs_desired;
  
  vec<chordID> failed;
  ptr<location> p = this->closestpred (ra->x, failed); // the next best guess

  // Update best guess or complete, depending, if successor is in our
  // successor list.
  if (betweenrightincl (myID, cs.back ()->id (), ra->x)) {
    // Calculate the amount of overlap available in the successor list
    size_t overlap = 0;
    size_t succind = 0;
    for (size_t i = 0; i < cs.size (); i++) {
      if (betweenrightincl (myID, cs[i]->id (), ra->x)) {
	// The i+1st successor is the key's successor!
	overlap = cs.size () - i;
	succind = i;
	break;
      }
    }
    rtrace << header << "overlap = " << overlap << " / m = " << m << "\n";

    // Try to decide who to talk to next.
    if (overlap >= m) {
      // Enough overlap to finish.
      cs.popn_front (succind); // leave succind+1st succ at front
      if (succind > 0)
	rtrace << myID << ": dorecroute (" << ra->routeid << ", "
	       << ra->x << "): skipping " << succind << " nodes.\n";
      
      dorecroute_sendcomplete (ra, cs);
      sbp->replyref (rstat);
      sbp = NULL;
      return;
    } else if (shave && ((int)m - (int)overlap < (int)cs.size ())) {
      // Override the absolute best we could've done, which probably
      // is the predecessor since our succlist spans the key, and
      // select someone nice and fast to get more successors from.
      float mindist = -1.0;
      size_t minind = 0;

      size_t start = m - overlap;
      strbuf distbuf;
      distbuf << "going to choose a distance from ";
      for (size_t i = start; i < cs.size (); i++) {
	float dist = Coord::distance_f (this->my_location ()->coords (),
					cs[i]->coords ());
	distbuf << cs[i]->id () << "(" << (int)dist << ") ";
	if (mindist < 0 || dist < mindist) {
	  mindist = dist;
	  minind  = i;
	}
      }
      distbuf << "; i chose " << cs[minind]->id () << "(" << (int)mindist << ")\n";
      rtrace << header << distbuf;
      if (minind < succind) {
	// This is proximity route selection!
	p = cs[minind];
      } else {
	ptr<location> nexthop = cs[minind];
	rtrace << header << "going for penult from " << nexthop->id () << "\n";
	cs.popn_front (succind); // just the overlap please
	dorecroute_sendpenult (ra, nexthop, p, cs);
	sbp->replyref (rstat);
	sbp = NULL;
	return;
      }
    }
  }
  
  dorecroute_sendroute (ra, p);
  sbp->replyref (rstat);
  sbp = NULL;
  return;
}

template<class T>
void
recroute<T>::dorecroute_sendpenult (recroute_route_arg *ra,
				    ptr<location> nexthop,
				    ptr<location> p,
				    vec<ptr<location> > cs)
{
  // Construct a new recroute_route_arg.
  chord_node_wire me;
  this->my_location ()->fill_node (me);
  
  ptr<recroute_penult_arg> nra = New refcounted<recroute_penult_arg> ();
  // XXX this is obnoxious
  nra->routeid = ra->routeid;
  nra->origin = ra->origin;
  nra->x = ra->x;
  nra->succs_desired = ra->succs_desired;

  nra->upcall_prog = ra->upcall_prog;
  nra->upcall_proc = ra->upcall_proc;
  nra->upcall_args = ra->upcall_args;

  nra->retries = ra->retries;

  nra->path.setsize (ra->path.size () + 1);
  for (size_t i = 0; i < ra->path.size (); i++) {
    nra->path[i] = ra->path[i];
  }
  nra->path[ra->path.size ()] = me;

  nra->successors.setsize (cs.size ());
  for (size_t i = 0; i < cs.size (); i++) {
    cs[i]->fill_node (nra->successors[i]);
  }
			    
  rtrace << this->my_ID () << ": dorecroute (" << ra->routeid << ", "
	 << ra->x << "): penultforwarding to " << nexthop->id () << "\n";

  vec<chordID> failed;
  failed.push_back (nexthop->id ());
  doRPC (nexthop, recroute_program_1, RECROUTEPROC_PENULTIMATE,
	 nra, NULL,
	 wrap (this, &recroute<T>::dorecroute_sendpenult_cb, nra, p, failed));
  // XXX support notification?
}

template<class T>
void
recroute<T>::dorecroute_sendpenult_cb (ptr<recroute_penult_arg> nra,
				       ptr<location> p,
				       vec<chordID> failed,
				       clnt_stat err)
{
  if (err) {
    // Go back to wherever we were going to go in the first place.
    // ra->retries++;
    // dorecroute_sendroute (ra, p);
    rtrace << this->my_ID () << ": dorecroute (" << nra->routeid << ", "
	   << nra->x << ": er. failure.....\n";
    return;
  }
}

template<class T>
void
recroute<T>::dorecroute_sendcomplete (recroute_route_arg *ra,
				      const vec<ptr<location> > cs)
{
  chord_node_wire me;
  this->my_location ()->fill_node (me);
  
  // If complete (i.e. we have enough here to satisfy request),
  // send off a complete RPC.
  rtrace << this->my_ID () << ": dorecroute (" << ra->routeid << ", " << ra->x
	 << "): complete.\n";

  ptr<recroute_complete_arg> ca = New refcounted<recroute_complete_arg> ();
  ca->body.set_status (RECROUTE_ROUTE_OK);
  ca->routeid = ra->routeid;
  
  ca->path.setsize (ra->path.size () + 1);
  for (size_t i = 0; i < ra->path.size (); i++) {
    ca->path[i] = ra->path[i];
  }
  ca->path[ra->path.size ()] = me;

  u_long m = ra->succs_desired;
  u_long tofill = (cs.size () < m) ? cs.size () : m;
  ca->body.robody->successors.setsize (tofill);
  for (size_t i = 0; i < tofill; i++)
    cs[i]->fill_node (ca->body.robody->successors[i]);

  ca->retries = ra->retries;
  
  ptr<location> l = this->locations->lookup_or_create (make_chord_node (ra->origin));
  doRPC (l, recroute_program_1, RECROUTEPROC_COMPLETE,
	 ca, NULL,
	 wrap (this, &recroute<T>::recroute_sent_complete_cb));
  // We don't really care if this is lost beyond the RPC system's
  // retransmits.
}

template<class T>
void
recroute<T>::dorecroute_sendroute (recroute_route_arg *ra, ptr<location> p)
{
  // Construct a new recroute_route_arg.
  chord_node_wire me;
  this->my_location ()->fill_node (me);
  
  ptr<recroute_route_arg> nra = New refcounted<recroute_route_arg> ();
  *nra = *ra;
  nra->path.setsize (ra->path.size () + 1);
  for (size_t i = 0; i < ra->path.size (); i++) {
    nra->path[i] = ra->path[i];
  }
  nra->path[ra->path.size ()] = me;
  
  if (p->id () != this->my_ID ()) {
    recroute_route_stat *res = New recroute_route_stat (RECROUTE_ACCEPTED);
    
    rtrace << this->my_ID () << ": dorecroute (" << ra->routeid << ", "
	   << ra->x << "): forwarding to " << p->id () << "\n";

    vec<chordID> failed;
    doRPC (p, recroute_program_1, RECROUTEPROC_ROUTE,
	   nra, res,
	   wrap (this, &recroute<T>::recroute_hop_cb, nra, p, failed, res),
	   wrap (this, &recroute<T>::recroute_hop_timeout_cb, nra, p, failed));
  } else {
    //XXX we're dropping this (instead of sending a failure message)
    //    since I'm sending a bunch of crazy parallel lookups and we're
    //    guessing that maybe another one will succeed.
    //    worst case: the lookup fails when the sweeper goes off.
    rtrace << this->my_ID () << " next hop is me. dropping\n";
  }
}

template<class T>
void
recroute<T>::recroute_hop_cb (ptr<recroute_route_arg> nra,
			      ptr<location> p,
			      vec<chordID> failed,
			      recroute_route_stat *res,
			      clnt_stat status)
{
  if (!status && *res == RECROUTE_ACCEPTED) {
    delete res; res = NULL;
    rtrace << this->my_ID () << ": dorecroute (" << nra->routeid << ", "
	   << nra->x << "): message accepted\n";
    return;
  }
  rtrace << this->my_ID () << ": dorecroute (" << nra->routeid << ", " << nra->x
	 << ") forwarding to "
	 << p->id () << " failed (" << status << "," << *res << ").\n";

  delete res;
  if (failed.size () > 3) {    // XXX hardcoded constant
    rtrace << this->my_ID () << ": dorecroute (" << nra->routeid << ", " << nra->x
	   << ") failed too often. Discarding.\n";
    
    
    ptr<recroute_complete_arg> ca = New refcounted<recroute_complete_arg> ();
    ca->body.set_status (RECROUTE_ROUTE_FAILED);
    p->fill_node (ca->body.rfbody->failed_hop);
    ca->body.rfbody->failed_stat = status;
    
    ca->routeid = nra->routeid;
    ca->retries = nra->retries;
    ca->path    = nra->path;

    ptr<location> o = this->locations->lookup_or_create (make_chord_node (nra->origin));
    doRPC (o, recroute_program_1, RECROUTEPROC_COMPLETE,
	   ca, NULL,
	   wrap (this, &recroute<T>::recroute_sent_complete_cb));
    // We don't really care if this is lost beyond the RPC system's
    // retransmits.
    return;
  }

}

template<class T>
bool
recroute<T>::recroute_hop_timeout_cb (ptr<recroute_route_arg> nra,
				      ptr<location> p,
				      vec<chordID> failed,
				      chord_node n,
				      int rexmit_number)
{
  if (rexmit_number == 0 && failed.size () <= 3)  {
    nra->retries++;
    failed.push_back (p->id ());
    ptr<location> l = this->closestpred (nra->x, failed);

    if (l->id () != this->my_ID ()) {
      
      rtrace << this->my_ID () << ": dorecroute (" << nra->routeid << ", "
	     << nra->x << "): TIMEOUT now forwarding to " << l->id () << "\n";
      recroute_route_stat *nres = New recroute_route_stat (RECROUTE_ACCEPTED);
      doRPC (p, recroute_program_1, RECROUTEPROC_ROUTE,
	     nra, nres,
	     wrap (this, &recroute<T>::recroute_hop_cb, nra, l, failed, nres),
	     wrap (this, &recroute<T>::recroute_hop_timeout_cb, nra, p, failed));
    } else {
      return false;
    }
  }
  return true;
}
template<class T>
void
recroute<T>::recroute_sent_complete_cb (clnt_stat status)
{
  if (status)
    rwarning << this->my_ID () << ": recroute_complete lost, status " << status << ".\n";
  // We're not going to do anything more clever about this. It's dropped. Fini.
}


template<class T>
void
recroute<T>::dopenult (user_args *sbp, recroute_penult_arg *ra)
{
  chord_node_wire me;
  this->my_location ()->fill_node (me);
  
  rtrace << this->my_ID () << ": dopenult (" << ra->routeid << ", " << ra->x
	 << "): complete.\n";

  ptr<recroute_complete_arg> ca = New refcounted<recroute_complete_arg> ();

  ca->routeid = ra->routeid;
  ca->path.setsize (ra->path.size () + 1);
  for (size_t i = 0; i < ra->path.size (); i++) {
    ca->path[i] = ra->path[i];
  }
  ca->path[ra->path.size ()] = me;
  ca->retries = ra->retries;

  u_long m = ra->succs_desired;
  vec<ptr<location> > cs = this->succs ();

  if (ra->successors.size () + cs.size () < m) {
    warn << "misdirected PENULT: " << ra->successors.size () << " + " << cs.size () << " < " << m << "\n";
    ca->body.set_status (RECROUTE_ROUTE_FAILED);
    ca->body.rfbody->failed_stat = RECROUTE_ROUTE_FAILED;
    this->my_location ()->fill_node(ca->body.rfbody->failed_hop);
  } else {
    ca->body.set_status (RECROUTE_ROUTE_OK);

    ca->body.robody->successors.setsize (m);
  
    size_t lastind = 0;
    for (size_t i = 0; i < ra->successors.size (); i++) {
      chord_node foo = make_chord_node (ra->successors[i]);
      //    rtrace << my_ID () << ": dopenult (" << ra->routeid
      //           << " filling " << i << "\n";
      ca->body.robody->successors[i] = ra->successors[i];
      if (foo.x == this->my_ID ()) {
	lastind = i;
	// rtrace << my_ID () << ": dopenult (" << ra->routeid << "): i'm at "
	//        << i << "\n";
	break;
      }
    }
    
    u_long tofill = m - (lastind + 1);
    if (tofill > cs.size ())
      tofill = cs.size ();
    
    for (size_t i = 0; i < tofill; i++) {
      //    rtrace << my_ID () << ": dopenult (" << ra->routeid
      //           << ") filling2 " << i + lastind + 1<< "\n";
      cs[i]->fill_node (ca->body.robody->successors[i + lastind + 1]);
    }
  
    ca->retries = ra->retries;
  }
  
  ptr<location> l = this->locations->lookup_or_create (make_chord_node (ra->origin));
  doRPC (l, recroute_program_1, RECROUTEPROC_COMPLETE,
	 ca, NULL,
	 wrap (this, &recroute<T>::recroute_sent_complete_cb));
  // We don't really care if this is lost beyond the RPC system's
  // retransmits.

  sbp->reply (NULL);
  sbp = NULL;
}

template<class T>
void
recroute<T>::docomplete (user_args *sbp, recroute_complete_arg *ca)
{
  rtrace << this->my_ID () << ": docomplete: routeid " << ca->routeid
	 << " has returned! retries: " << ca->retries << "\n";
  route_recchord *router = routers[ca->routeid];
  if (!router) {
    chord_node src; sbp->fill_from (&src);
    rtrace << this->my_ID () << ": docomplete: unknown routeid " << ca->routeid
		       << " from host " << src << "\n";
    sbp->reply (NULL);
    return;
  }
  
  routers.remove (router);
  router->handle_complete (sbp, ca);
}

template<class T>
void
recroute<T>::stats () const
{
  T::stats ();
  warnx << "Outstanding routing lookups:\n";
  route_recchord *ri = routers.first ();
  while (ri != NULL) {
    timespec ts = ri->start_time ();
    warnx << "  " << ri->routeid_ << " for " << ri->key ()
	  << " started " << ts.tv_sec << "." << ts.tv_nsec << "\n";
    ri = routers.next (ri);
  }
}

template<class T>
void
recroute<T>::find_succlist (const chordID &x, u_long m, cbroute_t cb,
			   ptr<chordID> guess)
{
  route_recchord *ri = static_cast<route_recchord *> (produce_iterator_ptr (x));
  if (shave) {
    rtrace << this->my_ID () << ": find_succlist (" << x << ", " << m
	   << ") is shaving.\n";
    ri->set_desired (m);
  }
  ri->first_hop (wrap (this, &recroute<T>::find_succlist_cb, cb, ri),
		 guess);
}

template<class T>
void
recroute<T>::find_succlist_cb (cbroute_t cb, route_recchord *ri, bool done)
{
  assert (done); // Expect that we are only called when totally done.
  vec<chord_node> cs = ri->successors ();
  cb (cs, ri->path (), ri->status ());
  delete ri;
  return;
}

// override produce_iterator*
template<class T>
ptr<route_iterator>
recroute<T>::produce_iterator (chordID xi) 
{
  ptr<route_recchord> ri = New refcounted<route_recchord> (mkref (this), xi);
  routers.insert (ri);
  return ri;
}

template<class T>
ptr<route_iterator>
recroute<T>::produce_iterator (chordID xi,
			       const rpc_program &uc_prog,
			       int uc_procno,
			       ptr<void> uc_args) 
{
  ptr<route_recchord> ri = New refcounted<route_recchord> (mkref (this),
							   xi, uc_prog,
							   uc_procno, uc_args);
  routers.insert (ri);
  return ri;
}

template<class T>
route_iterator *
recroute<T>::produce_iterator_ptr (chordID xi) 
{
  route_recchord *ri = New route_recchord (mkref (this), xi);
  routers.insert (ri);
  return ri;
}

template<class T>
route_iterator *
recroute<T>::produce_iterator_ptr (chordID xi,
				   const rpc_program &uc_prog,
				   int uc_procno,
				   ptr<void> uc_args) 
{
  route_recchord *ri = New route_recchord (mkref (this), xi, uc_prog,
					   uc_procno, uc_args);
  routers.insert (ri);
  return ri;
}

