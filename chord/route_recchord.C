#include <recroute_prot.h>
#include "route_recchord.h"
#include "chord_impl.h"
#include <misc_utils.h>
#include <location.h>
#include <locationtable.h>
#include <modlogger.h>
#ifdef DMALLOC
#include <dmalloc.h>
#endif

#define warning modlogger ("recroute", modlogger::WARNING)
#define info    modlogger ("recroute", modlogger::INFO)
#define trace   modlogger ("recroute", modlogger::TRACE)


/*
 * TODO
 * XXX Cleverly get the number of successors for desired_
 */
route_recchord::route_recchord (ptr<vnode> vi, chordID xi) : 
  route_iterator (vi, xi),
  desired_ (vi->succs ().size ()),
  routeid_ (get_nonce ())
{
  start_time_.tv_sec = 0;
}

route_recchord::route_recchord (ptr<vnode> vi, chordID xi,
				rpc_program uc_prog,
				int uc_procno,
				ptr<void> uc_args) : 
  route_iterator (vi, xi, uc_prog, uc_procno, uc_args),
  desired_ (vi->succs ().size ()),
  routeid_ (get_nonce ())
{
  start_time_.tv_sec = 0;
}

route_recchord::~route_recchord ()
{
}

// static class method
u_long
route_recchord::get_nonce ()
{
  return random_getword ();
}

bool
route_recchord::started () const
{
  return (start_time_.tv_sec > 0);
}

const timespec &
route_recchord::start_time () const
{
  return start_time_;
}

void
route_recchord::set_desired (u_long m)
{
  u_long ns = v->succs ().size (); // XXX export succ_list num_succ method?
  if (m > ns)
    m = ns;
  desired_ = m;
}

void
route_recchord::handle_timeout ()
{
  chordID myID = v->my_ID ();
  trace << myID << ": handle_timeout (" << routeid_ << ", " << x << ")\n";
  r = CHORD_RPCFAILURE;
  cb (true);
}

void
route_recchord::handle_complete (user_args *sbp, recroute_complete_arg *ca)
{
  // fill router with successor list and path from ca.
  search_path.clear ();
  for (size_t i = 0; i < ca->path.size (); i++) {
    ptr<location> l = v->locations->lookup_or_create
      (make_chord_node (ca->path[i]));
    if (!l) continue;
    search_path.push_back (l);
  }

  if (ca->body.status == RECROUTE_ROUTE_OK) {
    // Last path member should be successor, according to route_chord.
    ptr<location> n0 =
      v->locations->insert (make_chord_node (ca->body.robody->successors[0]));
    search_path.push_back (n0);
    
    successors_.clear ();

    for (size_t i = 0; i < ca->body.robody->successors.size (); i++) {
      chord_node s = make_chord_node (ca->body.robody->successors[i]);
      successors_.push_back (s);
    }
    r = CHORD_OK;
  } else {
    assert (ca->body.status == RECROUTE_ROUTE_FAILED);
    trace << "XXX. Log the failed_stat?\n";
    r = CHORD_RPCFAILURE;
  }
    
  sbp->reply (NULL);
  cb (true);
}

void
route_recchord::print ()
{
  // XXX
  fatal << "no print\n";
}

void
route_recchord::send (ptr<chordID> guess)
{
  fatal << "no send\n";
}

void
route_recchord::first_hop (cbhop_t cbi, ptr<chordID> guess)
{
  cb = cbi;

  ptr<recroute_route_arg> ra = New refcounted<recroute_route_arg> ();

  ra->routeid = routeid_;
  v->my_location ()->fill_node (ra->origin);
  ra->x = x;
  ra->retries = 0;
  ra->succs_desired = desired_;
  trace << v->my_ID () << ": new route_recchord::first_hop: desired = "
	<< desired_ << "\n";

  ra->upcall_prog = 0;
  ra->upcall_proc = 0;
  ra->upcall_args.setsize (0);

  ptr<location> p = v->closestpred (x, failed_nodes);
  recroute_route_stat *res = New recroute_route_stat (RECROUTE_ACCEPTED);

  v->doRPC (p, recroute_program_1, RECROUTEPROC_ROUTE,
	    ra, res,
	    wrap (this, &route_recchord::first_hop_cb, deleted, ra, res, p),
	    wrap (this, &route_recchord::timeout_cb, deleted, ra, p));
  
  clock_gettime (CLOCK_REALTIME, &start_time_);
}

bool
route_recchord::timeout_cb (ptr<bool> del,
			    ptr<recroute_route_arg> ra,
			    ptr<location> p,
			    chord_node n,
			    int retries)
{
  if (*del) return false;

  trace << v->my_ID () << ": first_hop (" << routeid_ << ", " << x
	<< ") forwarding to "
	<< p->id () << " TIMEOUT\n";
  if (retries == 0 && failed_nodes.size () <= 3) {
    failed_nodes.push_back (p->id ());
    ra->retries++;
    // XXX maybe strange things happen if p was my succ?
    ptr<location> np = v->closestpred (x, failed_nodes);
    recroute_route_stat *res = New recroute_route_stat (RECROUTE_ACCEPTED);
    v->doRPC (np, recroute_program_1, RECROUTEPROC_ROUTE,
	      ra, res,
	      wrap (this, &route_recchord::first_hop_cb, deleted, ra, res, np),
	      wrap (this, &route_recchord::timeout_cb, deleted, ra, np));  
  } 
  // cancel resends of this RPC. Note that we only retry the RPC here now.
  return true;
}

void
route_recchord::first_hop_cb (ptr<bool> del,
			      ptr<recroute_route_arg> ra,
			      recroute_route_stat *res,
			      ptr<location> p,
			      clnt_stat status)
{
  // XXX what a pain; this is rather similar to recroute::recroute_hop_cb
  //     should figure out if there is a clean way to abstract code.
  if (*del || ((status == RPC_SUCCESS) && (*res == RECROUTE_ACCEPTED))) {
    delete res;
    return;
  }

  chordID myID = v->my_ID ();
  trace << myID << ": first_hop (" << routeid_ << ", " << x
	<< ") forwarding to "
	<< p->id () << " failed (" << status << ", " << *res << ").\n";
  delete res;

  // XXX need to alert? probably not in recursive mode.
  if (failed_nodes.size () > 3) {    // XXX hardcoded constant
    trace << myID << ": first_hop (" << routeid_ << ", " << x
	  << ") failed too often. Discarding.\n";
    
    //Send myself a "complete" RPC indicating failure
    ptr<location> me = v->my_location ();
    ptr<recroute_complete_arg> ca = New refcounted<recroute_complete_arg> ();
    ca->body.set_status (RECROUTE_ROUTE_FAILED);
    me->fill_node (ca->body.rfbody->failed_hop);
    ca->body.rfbody->failed_stat = CHORD_RPCFAILURE;
    ca->routeid = routeid_;
    ca->retries = failed_nodes.size ();
    
    v->doRPC (me, recroute_program_1, RECROUTEPROC_COMPLETE,
	      ca, NULL, aclnt_cb_null);
    return;
  }

}

void
route_recchord::next_hop ()
{
  fatal << "no next_hop.\n";
}

ptr<location>
route_recchord::pop_back ()
{
  return search_path.pop_back ();
}
