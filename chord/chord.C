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

#include <assert.h>
#include <qhash.h>

#include "chord_impl.h"
#include "comm.h"
#include <coord.h>
#include <modlogger.h>
#include <misc_utils.h>
#include "chord_util.h"
#include <location.h>
#include <locationtable.h>

#include <configurator.h>

const int chord::max_vnodes = 1024;

const int CHORD_LOOKUP_FINGERLIKE (0);
const int CHORD_LOOKUP_LOCTABLE (1);
const int CHORD_LOOKUP_PROXIMITY (2);
const int CHORD_LOOKUP_FINGERSANDSUCCS (3);

ref<vnode>
vnode::produce_vnode (ptr<locationtable> _locations,
		      ptr<rpc_manager> _rpcm,
		      ptr<fingerlike> stab,
		      ptr<route_factory> f,
		      ptr<chord> _chordnode,
		      chordID _myID, int _vnode, int server_sel_mode,
		      int l_mode)
{
  return New refcounted<vnode_impl> (_locations, _rpcm,
				     stab, f, _chordnode, _myID,
				     _vnode, server_sel_mode, l_mode);
}

// Pure virtual destructors still need definitions
vnode::~vnode () {}

chordID
vnode_impl::my_ID () const
{
  return myID;
}

ref<location>
vnode_impl::my_location ()
{
  return me_;
}

vnode_impl::vnode_impl (ptr<locationtable> _locations,
			ptr<rpc_manager> _rpcm,
			ptr<fingerlike> stab,
			ptr<route_factory> f,
			ptr<chord> _chordnode,
			chordID _myID, int _vnode, int server_sel_mode,
			int l_mode) :
  rpcm (_rpcm),
  factory (f),
  myindex (_vnode),
  myID (_myID), 
  chordnode (_chordnode),
  server_selection_mode (server_sel_mode),
  lookup_mode (l_mode)
{
  locations = _locations;
  warnx << gettime () << " myID is " << myID << "\n";
  me_ = locations->lookup (myID);

  fingers = stab;
  fingers->init (mkref(this), locations);

  successors = New refcounted<succ_list> (mkref(this), locations);
  predecessors = New refcounted<pred_list> (mkref(this), locations);
  stabilizer = New refcounted<stabilize_manager> (myID);

  stabilizer->register_client (successors);
  stabilizer->register_client (predecessors);
  stabilizer->register_client (fingers);

  if (lookup_mode == CHORD_LOOKUP_PROXIMITY) {
    toes = New refcounted<toe_table> ();
    toes->init (mkref(this), locations);
    stabilizer->register_client (toes);
  } else {
    toes = NULL;
  }
    
  locations->incvnodes ();

  addHandler (chord_program_1, wrap(this, &vnode_impl::dispatch));

  ngetsuccessor = 0;
  ngetpredecessor = 0;
  ngetsucclist = 0;
  nfindsuccessor = 0;
  nhops = 0;
  nmaxhops = 0;
  nfindpredecessor = 0;
  nnotify = 0;
  nalert = 0;
  ntestrange = 0;
  ngetfingers = 0;
  
  ndogetsuccessor = 0;
  ndogetpredecessor = 0;
  ndonotify = 0;
  ndoalert = 0;
  ndogetsucclist = 0;
  ndotestrange = 0;
  ndogetfingers = 0;
  ndogetfingers_ext = 0;
  ndogetsucc_ext = 0;
  ndogetpred_ext = 0;
  ndogettoes = 0;
  ndofindtoes = 0;
  ndodebruijn = 0;

  delaycb (60, 0, wrap (this, &vnode_impl::check_dead_nodes));
}

vnode_impl::~vnode_impl ()
{
  warnx << myID << ": vnode_impl: destroyed\n";
  exit (0);
}

void
vnode_impl::dispatch (user_args *a)
{
  switch (a->procno) {
  case CHORDPROC_NULL: 
    {
      assert (0);
    }
    break;
  case CHORDPROC_GETSUCCESSOR:
    {
      doget_successor (a);
    }
    break;
  case CHORDPROC_GETPREDECESSOR:
    {
      doget_predecessor (a);
    }
    break;
  case CHORDPROC_NOTIFY:
    {
      chord_nodearg *na = a->template getarg<chord_nodearg> ();
      donotify (a, na);
    }
    break;
  case CHORDPROC_ALERT:
    {
      chord_nodearg *na = a->template getarg<chord_nodearg> ();
      doalert (a, na);
    }
    break;
  case CHORDPROC_GETSUCCLIST:
    {
      dogetsucclist (a);
    }
    break;
  case CHORDPROC_TESTRANGE_FINDCLOSESTPRED:
    {
      chord_testandfindarg *fa = a->template getarg<chord_testandfindarg> ();
      dotestrange_findclosestpred (a, fa);
    }
    break;
  case CHORDPROC_GETFINGERS: 
    {
      dogetfingers (a);
    }
    break;
  case CHORDPROC_GETFINGERS_EXT: 
    {
      dogetfingers_ext (a);
    }
    break;
  case CHORDPROC_GETPRED_EXT:
    {
      dogetpred_ext (a);
    }
    break;
  case CHORDPROC_GETSUCC_EXT:
    {
      dogetsucc_ext (a);
    }
    break;
  case CHORDPROC_SECFINDSUCC:
    {
      chord_testandfindarg *fa = 
	a->template getarg<chord_testandfindarg> ();
      dosecfindsucc (a, fa);
    }
    break;
  case CHORDPROC_GETTOES:
    {
      chord_gettoes_arg *ta = a->template getarg<chord_gettoes_arg> ();
      dogettoes (a, ta);
    }
    break;
  case CHORDPROC_DEBRUIJN:
    {
      chord_debruijnarg *da = 
	a->template getarg<chord_debruijnarg> ();
      dodebruijn (a, da);
    }
    break;
  case CHORDPROC_FINDROUTE:
    {
      chord_findarg *fa = a->template getarg<chord_findarg> ();
      dofindroute (a, fa);
    }
    break;
  case CHORDPROC_FINDTOES:
    {
      chord_findtoes_arg *ta = a->template getarg<chord_findtoes_arg> ();
      dofindtoes (a, ta);
    }    
    break;
  default:
    a->reject (PROC_UNAVAIL);
    break;
  }
}

ptr<location>
vnode_impl::my_pred() const
{
  return predecessors->pred ();
}

ptr<location>
vnode_impl::my_succ () const
{
  return successors->succ ();
}

ptr<route_factory>
vnode_impl::get_factory ()
{
  return factory;
}


void
vnode_impl::stats () const
{
  warnx << "VIRTUAL NODE STATS " << myID
	<< " stable? " << stabilizer->isstable () << "\n";
  warnx << "# estimated node in ring "
	<< locations->estimate_nodes () << "\n";
  warnx << "continuous_timer " << stabilizer->cts_timer ()
	<< " backoff " 	<< stabilizer->bo_timer () << "\n";
  
  warnx << "# getsuccesor requests " << ndogetsuccessor << "\n";
  warnx << "# getpredecessor requests " << ndogetpredecessor << "\n";
  warnx << "# getsucclist requests " << ndogetsucclist << "\n";
  warnx << "# notify requests " << ndonotify << "\n";  
  warnx << "# alert requests " << ndoalert << "\n";  
  warnx << "# testrange requests " << ndotestrange << "\n";  
  warnx << "# getfingers requests " << ndogetfingers << "\n";
  warnx << "# dodebruijn requests " << ndodebruijn << "\n";

  warnx << "# getsuccesor calls " << ngetsuccessor << "\n";
  warnx << "# getpredecessor calls " << ngetpredecessor << "\n";
  warnx << "# getsucclist calls " << ngetsucclist << "\n";
  warnx << "# findsuccessor calls " << nfindsuccessor << "\n";
  warnx << "# hops for findsuccessor " << nhops << "\n";
  {
    char buf[100];
    if (nfindsuccessor) {
      sprintf (buf, "   Average # hops: %f\n", ((float) nhops)/nfindsuccessor);
      warnx << buf;
    }
  }
  warnx << "   # max hops for findsuccessor " << nmaxhops << "\n";
  fingers->stats ();
  warnx << "# findpredecessor calls " << nfindpredecessor << "\n";
  warnx << "# rangandtest calls " << ntestrange << "\n";
  warnx << "# notify calls " << nnotify << "\n";  
  warnx << "# alert calls " << nalert << "\n";  
  warnx << "# getfingers calls " << ngetfingers << "\n";
}

void
vnode_impl::print (strbuf &outbuf) const
{
  outbuf << "======== " << myID << " ====\n";
  fingers->print (outbuf);
  successors->print (outbuf);

  outbuf << "pred : " << my_pred ()->id () << "\n";
  if (toes) {
    outbuf << "------------- toes ----------------------------------\n";
    toes->print (outbuf);
  }
  outbuf << "=====================================================\n";
}

ptr<location>
vnode_impl::lookup_closestsucc (const chordID &x)
{
  ptr<location> s;

  switch (lookup_mode) {
  case CHORD_LOOKUP_PROXIMITY:
    s = toes->closestsucc (x);
    break;
  case CHORD_LOOKUP_FINGERLIKE:
    s = fingers->closestsucc (x);
    break;
  case CHORD_LOOKUP_FINGERSANDSUCCS:
  case CHORD_LOOKUP_LOCTABLE:
    s = locations->closestsuccloc (x);
    break;
  default:
    assert (0 == "invalid lookup_mode");
  }
  return s;
}

static inline
bool candidate_is_closer (const ptr<location> &c, // new candidate node
			  const chordID &myID,
			  const chordID &x, // target
			  const vec<chordID> &failed, // avoid these
			  const vec<float> &qc, // their coords
			  float &mindist)   // previous best dist
{
  // Don't give back nodes that querier doesn't want.
  if (in_vector (failed, c->id ())) return false;
  // Do not overshoot, do not go backwards.
  if (!between (myID, x, c->id ())) return false;
      
  vec<float> them = c->coords ();
  if (!them.size ())
    return false; // XXX weird.
      
  // See if this improves the distance.
  float newdist = Coord::distance_f (qc, them);
  if (mindist < 0 || newdist < mindist) {
#if 0	
    char dstr[24];
    modlogger log = modlogger ("proximity-toe");
    log << "improving " << x << " from ";
    sprintf (dstr, "%10.2f", mindist);
    log << dstr << " to ";
    sprintf (dstr, "%10.2f", newdist);
    log << dstr << "\n";
#endif /* 0 */	
    mindist = newdist;
    return true;
  }
  return false;
}

static inline chordID
greedy_speed (const vec<float> &qc, // querier coordinates
	      const ptr<location> c,     // candidate
	      const chordID &myID)
{
  vec<float> them = c->coords ();
  if (!them.size ()) {
    modlogger ("greedy_speed") << "No coordinates for " << c->id () << "\n";
    return 0; // XXX weird
  }
  chordID id_dist = distance (myID, c->id ());
  float f = Coord::distance_f (qc, them);
  if (f < 1.0) f = 1.0;
  u_int32_t coord_dist = (u_int32_t) f;
  if (coord_dist == 0) {
    char buf[32];
    sprintf (buf, "%f", f);
    modlogger ("greedy_speed") << "dist " << buf << " gave cd = 0\n";
    return 0;
  }
  
  return id_dist / coord_dist;
}

ptr<location>
vnode_impl::closestgreedpred (const chordID &x, const vec<float> &n,
			      const vec<chordID> &failed)
{
  ptr<location> p = lookup_closestpred (x, failed); // fallback
  chordID bestspeed = 0;
  
  // the real "location table"
  vec<ref<fingerlike> > sources;
  sources.push_back (toes);
  sources.push_back (fingers);
  sources.push_back (successors);
  while (sources.size () > 0) {
    ref<fingerlike_iter> iter = sources.pop_front ()->get_iter ();
    while (!iter->done ()) {
      ptr<location> c = iter->next ();
      
      // Don't give back nodes that querier doesn't want.
      if (in_vector (failed, c->id ())) continue;
      // Do not overshoot, do not go backwards.
      if (!between (myID, x, c->id ())) continue;

      chordID speed = greedy_speed (n, c, myID);
      if (speed > bestspeed) {
	p = c;
	bestspeed = speed;
      }
    }
  }
  
  return p;
}

/*
 * Find the best proximity or ID space move we can make for the chord
 * querier located at n, towards x.
 *
 * This code assumes that myID is not the predecessor of x; in this
 * case, testrange should be "inrange" and not doing this stuff.
 */
ptr<location>
vnode_impl::closestproxpred (const chordID &x, const vec<float> &n,
			     const vec<chordID> &failed)
{
  ptr<location> p = me_;
  
  float mindist = -1.0;

  ref<fingerlike_iter> iter = toes->get_iter ();
  while (!iter->done ()) {
    ptr<location> c = iter->next ();
    if (candidate_is_closer (c, myID, x, failed, n, mindist))
      p = c;
  }
  // We have a toe that makes progress and is acceptable to the
  // querier, let's go for it.
  if (mindist >= 0.0)
    return p;
    
  // No good toes?  Either we are too close, or they were all rejected.
  // If we happen to span the key in our successor _list_, then
  // attempt to send to some close node in the last half of the
  // successor list, so that for fetches, you will almost definitely win.
  vec<ptr<location> > sl = succs ();
  size_t sz = sl.size ();
  if (sz > 1 && between (sl[0]->id (), sl[sz - 1]->id (), x)) {
    for (u_int i = 0; i < sz; i++) {
      if (candidate_is_closer (sl[i], myID, x, failed, n, mindist))
	p = sl[i];
    }
  }
  if (mindist >= 0.0)
    return p;

  // Okay, we are just too far away, let's just go as far as we can
  // with the fingers.
  ptr<location> f = fingers->closestpred (x, failed);
  ptr<location> u = successors->closestpred (x, failed);
  if (between (myID, f->id (), u->id ())) 
    p = f;
  else
    p = u;
  
  return p;
}

ptr<location> 
vnode_impl::lookup_closestpred (const chordID &x, const vec<chordID> &failed)
{
  ptr<location> s;
  
  switch (lookup_mode) {
  case CHORD_LOOKUP_PROXIMITY:
    {
      s = closestproxpred (x, me_->coords (), failed);
      break;
    }
  case CHORD_LOOKUP_FINGERLIKE:
    s = fingers->closestpred (x, failed);
    break;
  case CHORD_LOOKUP_FINGERSANDSUCCS:
    {
      ptr<location> f = fingers->closestpred (x, failed);
      ptr<location> u = successors->closestpred (x, failed);
      if (between (myID, f->id (), u->id ())) 
	s = f;
      else
	s = u;
      break;
    }
  case CHORD_LOOKUP_LOCTABLE:
    s = locations->closestpredloc (x, failed);
    break;
  default:
    assert (0 == "invalid lookup_mode");
  }

  return s;
}


ptr<location>
vnode_impl::lookup_closestpred (const chordID &x)
{
  ptr<location> s;
  
  switch (lookup_mode) {
  case CHORD_LOOKUP_PROXIMITY:
    {
      vec<chordID> failed;
      s = closestproxpred (x, me_->coords (), failed);
      break;
    }
  case CHORD_LOOKUP_FINGERLIKE:
    s = fingers->closestpred (x);
    break;
  case CHORD_LOOKUP_FINGERSANDSUCCS:
    {
      ptr<location> f = fingers->closestpred (x);
      ptr<location> u = successors->closestpred (x);
      if (between (myID, f->id (), u->id ())) 
	s = f;
      else
	s = u;
      break;
    }
  case CHORD_LOOKUP_LOCTABLE:
    s = locations->closestpredloc (x);
    break;
  default:
    assert (0 == "invalid lookup_mode");
  }

  return s;
}

void
vnode_impl::stabilize (void)
{
  stabilizer->start ();
}

void
vnode_impl::join (ptr<location> n, cbjoin_t cb)
{
  ptr<chord_findarg> fa = New refcounted<chord_findarg> ();
  fa->x = incID (myID);
  chord_nodelistres *route = New chord_nodelistres ();
  doRPC (n, chord_program_1, CHORDPROC_FINDROUTE, fa, route,
	 wrap (this, &vnode_impl::join_getsucc_cb, n, cb, route));
}

void 
vnode_impl::join_getsucc_cb (ptr<location> n,
			     cbjoin_t cb, chord_nodelistres *route,
			     clnt_stat err)
{
  ptr<vnode> v = NULL;
  chordstat status = CHORD_OK;
  
  if (err) {
    warnx << myID << ": join RPC failed: " << err << "\n";
    if (err == RPC_TIMEDOUT) {
      // try again. XXX limit the number of retries??
      join (n, cb);
      return;
    }
    status = CHORD_RPCFAILURE;
  } else if (route->status != CHORD_OK) {
    status = route->status;
  } else if (route->resok->nlist.size () < 1) {
    status = CHORD_ERRNOENT;
  } else {
    // Just insert a possible predecessor and successor.
    size_t i = route->resok->nlist.size () - 1;
    for (size_t j = 0; j < 2 && i >= 0; j++) {
      locations->insert (make_chord_node (route->resok->nlist[i]));
      i--;
    }
    stabilize ();
    notify (my_succ (), myID);
    v = mkref (this);
    status = CHORD_OK;
  }
  if (status != CHORD_OK) {
    // XXX
    warnx << myID << ": should remove me from vnodes since I failed to join.\n";
  }
  cb (v, status);
  delete route;
}

void
vnode_impl::doget_successor (user_args *sbp)
{
  ndogetsuccessor++;
  
  ptr<location> s = my_succ ();
  chord_noderes res(CHORD_OK);
  s->fill_node (*res.resok);
  sbp->reply (&res);
}

void
vnode_impl::doget_predecessor (user_args *sbp)
{
  ndogetpredecessor++;
  ptr<location> p = my_pred ();
  chord_noderes res(CHORD_OK);
  p->fill_node (*res.resok);
  sbp->reply (&res);
}

void
vnode_impl::do_upcall_cb (char *a, int upcall_prog, int upcall_proc,
			  cbupcalldone_t done_cb, bool v)
{
  const rpc_program *prog = chordnode->get_program (upcall_prog);
  assert (prog);
  xdr_delete (prog->tbl[upcall_proc].xdr_arg, a);
  done_cb (v);
}

void
vnode_impl::do_upcall (int upcall_prog, int upcall_proc,
		  void *uc_args, int uc_args_len,
		  cbupcalldone_t done_cb)

{
  upcall_record *uc = upcall_table[upcall_prog];
  if (!uc) { 
    warn << "upcall not registered\n";
    done_cb (false);
    return;
  }

  const rpc_program *prog = chordnode->get_program (upcall_prog);
  if (!prog) 
    fatal << "bad prog: " << upcall_prog << "\n";

  
  xdrmem x ((char *)uc_args, uc_args_len, XDR_DECODE);
  xdrproc_t proc = prog->tbl[upcall_proc].xdr_arg;
  assert (proc);
  
  char *unmarshalled_args = (char *)prog->tbl[upcall_proc].alloc_arg ();
  if (!proc (x.xdrp (), unmarshalled_args))
    fatal << "upcall: error unmarshalling arguments\n";
  
  //run the upcall. It returns a pointer to its result and a length in the cb
  cbupcall_t cb = uc->cb;
  (*cb)(upcall_proc, (void *)unmarshalled_args,
	wrap (this, &vnode_impl::do_upcall_cb, 
	      unmarshalled_args, upcall_prog, upcall_proc,
	      done_cb));

}

void
vnode_impl::dotestrange_findclosestpred (user_args *sbp, chord_testandfindarg *fa) 
{
  ndotestrange++;
  chordID x = fa->x;
  chordID succ = my_succ ()->id ();

  chord_testandfindres *res = New chord_testandfindres ();  
  if (betweenrightincl(myID, succ, x) ) {
    res->set_status (CHORD_INRANGE);
    ref<fingerlike_iter> iter = successors->get_iter ();
    res->inrange->n.setsize (iter->size ());
    size_t i = 0;
    while (!iter->done ()) {
      ptr<location> n = iter->next ();
      n->fill_node (res->inrange->n[i++]);
    }
  } else {
    res->set_status (CHORD_NOTINRANGE);
    vec<chordID> f;
    for (unsigned int i=0; i < fa->failed_nodes.size (); i++)
      f.push_back (fa->failed_nodes[i]);
    ptr<location> p;
    if (server_selection_mode & 2) {
      p = closestgreedpred (fa->x, convert_coords (sbp->transport_header ()),
			    f);
    } else if (lookup_mode == CHORD_LOOKUP_PROXIMITY) {
      // Don't use lookup_closestpred which returns things from the
      // point of view of this node.
      p = closestproxpred (fa->x, convert_coords (sbp->transport_header ()),
			   f);
    } else {
      p = lookup_closestpred (fa->x, f);
    }
    p->fill_node (res->notinrange->n);
    
    ref<fingerlike_iter> iter = successors->get_iter ();
    res->notinrange->succs.setsize (iter->size ());
    size_t i = 0;
    while (!iter->done ()) {
      ptr<location> n = iter->next ();
      n->fill_node (res->notinrange->succs[i++]);
    }
  }

  if (fa->upcall_prog)  {
    do_upcall (fa->upcall_prog, fa->upcall_proc,
	       fa->upcall_args.base (), fa->upcall_args.size (),
	       wrap (this, &vnode_impl::chord_upcall_done, fa, res, sbp));

  } else {
    sbp->reply (res);
    delete res;
  }
}

void
vnode_impl::chord_upcall_done (chord_testandfindarg *fa,
			  chord_testandfindres *res,
			  user_args *sbp,
			  bool stop)
{
  
  if (stop) res->set_status (CHORD_STOP);
  sbp->reply (res);
  delete res;
}

void
vnode_impl::donotify (user_args *sbp, chord_nodearg *na)
{
  ndonotify++;
  predecessors->update_pred (make_chord_node (na->n));
  chordstat res = CHORD_OK;
  sbp->reply (&res);
}

void
vnode_impl::doalert (user_args *sbp, chord_nodearg *na)
{
  ndoalert++;
  chord_node n = make_chord_node (na->n);
  ptr<location> l = locations->lookup (n.x);
  if (l) {
    // check whether we cannot reach x either
    chord_noderes *res = New chord_noderes (CHORD_OK);
    ptr<chordID> v = New refcounted<chordID> (n.x);
    doRPC (l, chord_program_1, CHORDPROC_GETSUCCESSOR, v, res,
	   wrap (mkref (this), &vnode_impl::doalert_cb, res, n.x));
  }
  chordstat res = CHORD_OK;
  sbp->reply (&res);
}

void
vnode_impl::doalert_cb (chord_noderes *res, chordID x, clnt_stat err)
{
  if (!err) {
    warnx << "doalert_cb: " << x << " is alive\n";
  } else {
    warnx << "doalert_cb: " << x << " is indeed not alive\n";
    // doRPCcb has already killed this node for us.
  }
}

void
vnode_impl::dogetfingers (user_args *sbp)
{
  chord_nodelistres res(CHORD_OK);
  ndogetfingers++;
  fingers->fill_nodelistres (&res);
  sbp->reply (&res);
}


void
vnode_impl::dogetfingers_ext (user_args *sbp)
{
  chord_nodelistextres res(CHORD_OK);
  ndogetfingers_ext++;

  fingers->fill_nodelistresext (&res);

  sbp->reply (&res);
}

void
vnode_impl::dogetsucc_ext (user_args *sbp)
{
  chord_nodelistextres res(CHORD_OK);
  ndogetsucc_ext++;
  successors->fill_nodelistresext (&res);
  sbp->reply (&res);
}

void
vnode_impl::dogetpred_ext (user_args *sbp)
{
  ndogetpred_ext++;
  chord_nodeextres res(CHORD_OK);
  ptr<location> p = my_pred ();
  p->fill_node_ext(*res.resok);
  sbp->reply (&res);
}

void
vnode_impl::dosecfindsucc (user_args *sbp, chord_testandfindarg *fa)
{
  int nsucc;
  bool ok = Configurator::only ().get_int ("chord.nsucc", nsucc);
  assert (ok);
  
  size_t i = 0;
  chord_nodelistres *res = New chord_nodelistres (CHORD_OK);
  ptr<location> s = fingers->closestpred (fa->x);
  // XXX what if there aren't that many truely maintained successors
  //     in the location table???
  ptr<location> start = s;

  vec<chord_node_wire> answers;
  chord_node_wire n;
  for (i = 0; i < static_cast<unsigned> (nsucc); i++) {
    s->fill_node (n);
    answers.push_back (n);
    s = locations->closestsuccloc (incID (s->id ()));
    if (s == start) break;
  }
  res->resok->nlist.setsize (answers.size ());
  for (i = 0; i < answers.size (); i++)
    res->resok->nlist[i] = answers[i];

  if (fa->upcall_prog) {
    warnx << myID << ": doing upcall for " << fa->x << "\n";
    do_upcall (fa->upcall_prog, fa->upcall_proc,
	       fa->upcall_args.base (), fa->upcall_args.size (),
	       wrap (this, &vnode_impl::secchord_upcall_done, res, sbp));
  } else {
    sbp->reply (&res);
    delete res;
  }
}

void
vnode_impl::secchord_upcall_done (chord_nodelistres *res, user_args *sbp,
				  bool stop)
{
  if (stop) 
    warnx << "secchord_upcall_done would've told someone to stop searching...\n";
  
  sbp->reply (&res);
  delete res;
}

//find toes approprate for n's table
void
vnode_impl::dofindtoes (user_args *sbp, chord_findtoes_arg *ta)
{
  chord_nodelistres res (CHORD_OK);
  vec<chordID> r;
  vec<ptr<location> > rr;
  vec<float> coords;
  chord_node_wire n;
  unsigned int maxret;

  ndofindtoes++;

  if(toes){
    float maxd = toes->level_to_delay(ta->level);
    n = ta->n;
    maxret = toes->get_target_size(0);
    for(unsigned int i = 0; i < n.coords.size (); i++)
      coords.push_back((float)n.coords[i]);

    //iterate through toe table and return at most distance away from n
    for(unsigned int l = 0; l < MAX_LEVELS; l++){
      vec<ptr<location> > t = toes->get_toes(l);      
      for(unsigned int i = 0; i < t.size(); i++){
	if(in_vector(r, t[i]->id ()))
	   continue;
	if (Coord::distance_f (coords, t[i]->coords()) < maxd){ 
	  r.push_back(t[i]->id ());
	  rr.push_back (t[i]);
	  if(r.size() >= maxret)
	    break;
	}
      }
      if(r.size() >= maxret)
	break;
    }

    //warn << "find toes found " << r.size() << "\n";
    
    res.resok->nlist.setsize (r.size ());
    for (unsigned int i = 0; i < r.size (); i++)
      rr[i]->fill_node (res.resok->nlist[i]);
  } 
  
  //for(unsigned int i = 0 ; i < res.resok->nlist.size() ; i++){
  //  warn << "node " << res.resok->nlist[i].x << "\n";
  //}

  sbp->reply (&res);
  
}

//return my table
void
vnode_impl::dogettoes (user_args *sbp, chord_gettoes_arg *ta)
{

  chord_nodelistextres res (CHORD_OK);
  ndogettoes++;
  if(toes)
    toes->fill_nodelistresext (&res);

  sbp->reply (&res);
}

void
vnode_impl::dogetsucclist (user_args *sbp)
{
  ndogetsucclist++;
  chord_nodelistres res (CHORD_OK);
  successors->fill_nodelistres (&res);
  sbp->reply (&res);
}

void
vnode_impl::dodebruijn (user_args *sbp, chord_debruijnarg *da)
{
  ndodebruijn++;
  chord_debruijnres *res;
  ptr<location> succ = my_succ ();

  //  warnx << myID << " dodebruijn: succ " << succ << " x " << da->x << " i " 
  // << da->i << " between " << betweenrightincl (myID, succ, da->i) 
  // << " k " << da->k << "\n";

  res = New chord_debruijnres ();
  if (betweenrightincl (myID, succ->id (), da->x)) {
    res->set_status(CHORD_INRANGE);
    succ->fill_node (res->inres->node);
    ref<fingerlike_iter> iter = successors->get_iter ();
    res->inres->succs.setsize (iter->size ());
    size_t i = 0;
    while (!iter->done ()) {
      ptr<location> n = iter->next ();
      n->fill_node (res->inres->succs[i++]);
    }
  } else {
    res->set_status (CHORD_NOTINRANGE);
    if (betweenrightincl (myID, succ->id (), da->i)) {
      // ptr<debruijn> d = dynamic_cast< ptr<debruijn> >(fingers);
      // assert (d);  // XXXX return error
      // chordID nd =  d->debruijnprt (); 
      ptr<location> nd = lookup_closestpred (doubleID (myID, logbase));
      nd->fill_node (res->noderes->node);
      res->noderes->i = doubleID (da->i, logbase);
      res->noderes->i = res->noderes->i | topbits (logbase, da->k);
      res->noderes->k = shifttopbitout (logbase, da->k);
    } else {
      ptr<location> x = lookup_closestpred (da->i); // succ
      x->fill_node (res->noderes->node);
      res->noderes->i = da->i;
      res->noderes->k = da->k;
    }
    ref<fingerlike_iter> iter = successors->get_iter ();
    res->noderes->succs.setsize (iter->size ());
    size_t i = 0;
    while (!iter->done ()) {
      ptr<location> n = iter->next ();
      n->fill_node (res->noderes->succs[i++]);
    }
  }

  if (da->upcall_prog)  {
    do_upcall (da->upcall_prog, da->upcall_proc,
	       da->upcall_args.base (), da->upcall_args.size (),
	       wrap (this, &vnode_impl::debruijn_upcall_done, da, res, sbp));
    
  } else {
    sbp->reply (res);
    delete res;
  }
}

void
vnode_impl::debruijn_upcall_done (chord_debruijnarg *da,
			     chord_debruijnres *res,
			     user_args *sbp,
			     bool stop)
{
  
  if (stop) res->set_status (CHORD_STOP);
  sbp->reply(res);
  delete res;
}

void
vnode_impl::dofindroute (user_args *sbp, chord_findarg *fa)
{
  find_route (fa->x, wrap (this, &vnode_impl::dofindroute_cb, sbp, fa));
}

void
vnode_impl::dofindroute_cb (user_args *sbp, chord_findarg *fa, 
			    vec<chord_node> s, route r, chordstat err)
{
  if (err) {
    chord_nodelistres res (CHORD_RPCFAILURE);
    sbp->reply (&res);
  } else {
    chord_nodelistres res (CHORD_OK);
    res.resok->nlist.setsize (r.size ());
    for (unsigned int i = 0; i < r.size (); i++)
      r[i]->fill_node (res.resok->nlist[i]);
    sbp->reply (&res);
  }
}

void
vnode_impl::stop (void)
{
  stabilizer->stop ();
}
  
