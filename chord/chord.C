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

const int chord::max_vnodes = 1024;

const int CHORD_LOOKUP_FINGERLIKE (0);
const int CHORD_LOOKUP_LOCTABLE (1);
const int CHORD_LOOKUP_PROXIMITY (2);
const int CHORD_LOOKUP_FINGERSANDSUCCS (3);

ref<vnode>
vnode::produce_vnode (ptr<locationtable> _locations, ptr<fingerlike> stab,
			ptr<route_factory> f,
			ptr<chord> _chordnode,
			chordID _myID, int _vnode, int server_sel_mode,
			int l_mode)
{
  return New refcounted<vnode_impl> (_locations, stab, f, _chordnode, _myID,
				     _vnode, server_sel_mode, l_mode);
}

// Pure virtual destructors still need definitions
vnode::~vnode () {}

chordID
vnode_impl::my_ID () const
{
  return myID;
}

vnode_impl::vnode_impl (ptr<locationtable> _locations, ptr<fingerlike> stab,
			ptr<route_factory> f,
			ptr<chord> _chordnode,
			chordID _myID, int _vnode, int server_sel_mode,
			int l_mode) :
  myindex (_vnode),
  myID (_myID), 
  chordnode (_chordnode),
  factory (f),
  server_selection_mode (server_sel_mode),
  lookup_mode (l_mode)
{
  locations = _locations;
  warnx << gettime () << " myID is " << myID << "\n";

  vec<float> coords;
  warn << gettime () << " coords are: ";

  for (int i = 0; i < chord::NCOORDS; i++) {
    coords.push_back (uniform_random_f (1000.0));
    warnx << (int)coords.back () << " " ;
  }
  warnx << "\n";
  locations->set_coords (myID, coords);

  fingers = stab;
  fingers->init (mkref(this), locations, myID);

  successors = New refcounted<succ_list> (mkref(this), locations, myID);
  predecessors = New refcounted<pred_list> (mkref(this), locations, myID);
  stabilizer = New refcounted<stabilize_manager> (myID);

  stabilizer->register_client (successors);
  stabilizer->register_client (predecessors);
  stabilizer->register_client (fingers);

  if (lookup_mode == CHORD_LOOKUP_PROXIMITY) {
    toes = New refcounted<toe_table> ();
    toes->init (mkref(this), locations, myID);
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
  nfindsuccessorrestart = 0;
  nfindpredecessorrestart = 0;
  nnotify = 0;
  nalert = 0;
  ntestrange = 0;
  ngetfingers = 0;
  
  ndogetsuccessor = 0;
  ndogetpredecessor = 0;
  ndofindclosestpred = 0;
  ndonotify = 0;
  ndoalert = 0;
  ndogetsucclist = 0;
  ndotestrange = 0;
  ndogetfingers = 0;
  ndogetfingers_ext = 0;
  ndogetsucc_ext = 0;
  ndogetpred_ext = 0;
  ndochallenge = 0;
  ndogettoes = 0;
  ndodebruijn = 0;
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
  default:
    a->reject (PROC_UNAVAIL);
    break;
  }
}

chordID
vnode_impl::my_pred() const
{
  return predecessors->pred ();
}

chordID
vnode_impl::my_succ () const
{
  return successors->succ ();
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
  warnx << "# findclosestpred requests " << ndofindclosestpred << "\n";
  warnx << "# getsucclist requests " << ndogetsucclist << "\n";
  warnx << "# notify requests " << ndonotify << "\n";  
  warnx << "# alert requests " << ndoalert << "\n";  
  warnx << "# testrange requests " << ndotestrange << "\n";  
  warnx << "# getfingers requests " << ndogetfingers << "\n";
  warnx << "# dochallenge requests " << ndochallenge << "\n";
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
  warnx << "# findsuccessorrestart calls " << nfindsuccessorrestart << "\n";
  warnx << "# findpredecessorrestart calls " << nfindpredecessorrestart << "\n";
  warnx << "# rangandtest calls " << ntestrange << "\n";
  warnx << "# notify calls " << nnotify << "\n";  
  warnx << "# alert calls " << nalert << "\n";  
  warnx << "# getfingers calls " << ngetfingers << "\n";
}

void
vnode_impl::print () const
{
  warnx << "======== " << myID << "====\n";
  fingers->print ();
  successors->print ();

  warnx << "pred : " << my_pred () << "\n";
  if (toes) {
    warnx << "------------- toes ----------------------------------\n";
    toes->dump ();
  }
  warnx << "=====================================================\n";



}

chordID
vnode_impl::lookup_closestsucc (const chordID &x)
{
  chordID s;

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

chordID
vnode_impl::lookup_closestpred (const chordID &x, vec<chordID> failed_nodes)
{
  chordID s;
  
  switch (lookup_mode) {
  case CHORD_LOOKUP_PROXIMITY:
    s = toes->closestpred (x, failed_nodes);
    break;
  case CHORD_LOOKUP_FINGERLIKE:
    s = fingers->closestpred (x, failed_nodes);
    break;
  case CHORD_LOOKUP_FINGERSANDSUCCS:
    {
      chordID f = fingers->closestpred (x, failed_nodes);
      chordID u = successors->closestpred (x, failed_nodes);
      if (between (myID, f, u)) 
	s = f;
      else
	s = u;
      break;
    }
  case CHORD_LOOKUP_LOCTABLE:
    s = locations->closestpredloc (x, failed_nodes);
    break;
  default:
    assert (0 == "invalid lookup_mode");
  }

  return s;
}


chordID
vnode_impl::lookup_closestpred (const chordID &x)
{
  chordID s;
  
  switch (lookup_mode) {
  case CHORD_LOOKUP_PROXIMITY:
    s = toes->closestpred (x);
    break;
  case CHORD_LOOKUP_FINGERLIKE:
    s = fingers->closestpred (x);
    break;
  case CHORD_LOOKUP_FINGERSANDSUCCS:
    {
      chordID f = fingers->closestpred (x);
      chordID u = successors->closestpred (x);
      if (between (myID, f, u)) 
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
vnode_impl::join (const chord_node &n, cbjoin_t cb)
{
  ptr<chord_findarg> fa = New refcounted<chord_findarg> ();
  fa->x = incID (myID);
  chord_nodelistres *route = New chord_nodelistres ();
  doRPC (n, chord_program_1, CHORDPROC_FINDROUTE, fa, route,
	 wrap (this, &vnode_impl::join_getsucc_cb, n, cb, route));
}

void 
vnode_impl::join_getsucc_cb (const chord_node n,
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
    for (size_t i = 0; i < route->resok->nlist.size(); i++) {
      locations->insert (route->resok->nlist[i]);
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
  
  chordID s = successors->succ ();
  chord_noderes res(CHORD_OK);
  bool ok = locations->get_node (s, res.resok);
  assert (ok);
  sbp->reply (&res);
}

void
vnode_impl::doget_predecessor (user_args *sbp)
{
  ndogetpredecessor++;
  chordID p = my_pred ();
  chord_noderes res(CHORD_OK);
  bool ok = locations->get_node (p, res.resok);
  assert (ok);
  sbp->reply (&res);
}

void
vnode_impl::do_upcall_cb (char *a, cbupcalldone_t done_cb, bool v)
{
  delete[] a;
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
	wrap (this, &vnode_impl::do_upcall_cb, unmarshalled_args, done_cb));

}

void
vnode_impl::dotestrange_findclosestpred (user_args *sbp, chord_testandfindarg *fa) 
{
  ndotestrange++;
  chordID x = fa->x;
  chordID succ = my_succ ();

  chord_testandfindres *res = New chord_testandfindres ();  
  if (betweenrightincl(myID, succ, x) ) {
    res->set_status (CHORD_INRANGE);
    bool ok = locations->get_node (succ, &res->inrange->n);
    assert (ok);
  } else {
    res->set_status (CHORD_NOTINRANGE);
    vec<chordID> f;
    for (unsigned int i=0; i < fa->failed_nodes.size (); i++)
      f.push_back (fa->failed_nodes[i]);
    chordID p = lookup_closestpred (fa->x, f);
    bool ok = locations->get_node (p, &res->notinrange->n);
    assert (ok);
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
vnode_impl::dofindclosestpred (user_args *sbp, chord_findarg *fa)
{
  chord_noderes res(CHORD_OK);
  chordID p = lookup_closestpred (fa->x);
  ndofindclosestpred++;
  bool ok = locations->get_node (p, res.resok);
  assert (ok);
  assert (0);
  //  sbp->reply (&res);
}

void
vnode_impl::donotify (user_args *sbp, chord_nodearg *na)
{
  ndonotify++;
  predecessors->update_pred (na->n.x, na->n.r);
  chordstat res = CHORD_OK;
  sbp->reply (&res);
}

void
vnode_impl::doalert (user_args *sbp, chord_nodearg *na)
{
  ndoalert++;
  if (locations->cached (na->n.x)) {
    // check whether we cannot reach x either
    chord_noderes *res = New chord_noderes (CHORD_OK);
    ptr<chordID> v = New refcounted<chordID> (na->n.x);
    doRPC (na->n.x, chord_program_1, CHORDPROC_GETSUCCESSOR, v, res,
	   wrap (mkref (this), &vnode_impl::doalert_cb, res, na->n.x));
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
  res.resok->alive = true;
  locations->fill_getnodeext (*res.resok, my_pred ());
  sbp->reply (&res);
}

void
vnode_impl::dosecfindsucc (user_args *sbp, chord_testandfindarg *fa)
{
  size_t i = 0;
  chord_nodelistres *res = New chord_nodelistres (CHORD_OK);
  chordID s = fingers->closestpred (fa->x);
  // XXX what if there aren't that many truely maintained successors
  //     in the location table???
  chordID start = s;

  vec<chord_node> answers;
  chord_node n;
  for (i = 0; i < NSUCC; i++) {
    locations->get_node (s, &n);
    answers.push_back (n);
    s = locations->closestsuccloc (incID (s));
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

void
vnode_impl::dogettoes (user_args *sbp, chord_gettoes_arg *ta)
{

  chord_nodelistextres res (CHORD_OK);
  if(toes){
    vec<chordID> t = toes->get_toes (ta->level);
    
    ndogettoes++;
    res.resok->nlist.setsize (t.size ());
    for (unsigned int i = 0; i < t.size (); i++) {
      locations->fill_getnodeext (res.resok->nlist[i], t[i]);
    }
  } 

  sbp->reply (&res);
}

void
vnode_impl::dogetsucclist (user_args *sbp)
{
  ndogetsucclist++;
  chord_nodelistres res (CHORD_OK);

  vec<chord_node> s = succs ();
  chord_node self;
  bool ok = locations->get_node (my_ID (), &self);
  assert (ok);

  // the succs we send back starts with 'us'
  res.resok->nlist.setsize (1 + s.size ());
  res.resok->nlist[0] = self;
  for (u_int i = 0; i < s.size (); i++)
    res.resok->nlist[i + 1] = s[i];

  sbp->reply (&res);
}

void
vnode_impl::dodebruijn (user_args *sbp, chord_debruijnarg *da)
{
  ndodebruijn++;
  chord_debruijnres *res;
  chordID succ = my_succ ();

  //  warnx << myID << " dodebruijn: succ " << succ << " x " << da->x << " i " 
  // << da->i << " between " << betweenrightincl (myID, succ, da->i) 
  // << " k " << da->k << "\n";

  res = New chord_debruijnres ();
  if (betweenrightincl (myID, succ, da->x)) {
    res->set_status(CHORD_INRANGE);
    bool ok = locations->get_node (succ, &res->inres->node);
    assert (ok);
  } else {
    res->set_status (CHORD_NOTINRANGE);
    if (betweenrightincl (myID, succ, da->i)) {
      // ptr<debruijn> d = dynamic_cast< ptr<debruijn> >(fingers);
      // assert (d);  // XXXX return error
      // chordID nd =  d->debruijnprt (); 
      chordID nd = lookup_closestpred (doubleID (myID, logbase));
      bool ok = locations->get_node (nd, &res->noderes->node);
      assert (ok);
      res->noderes->i = doubleID (da->i, logbase);
      res->noderes->i = res->noderes->i | topbits (logbase, da->k);
      res->noderes->k = shifttopbitout (logbase, da->k);
    } else {
      chordID x = lookup_closestpred (da->i); // succ
      bool ok = locations->get_node (x, &res->noderes->node);
      assert (ok);
      res->noderes->i = da->i;
      res->noderes->k = da->k;
    }
  }

  if (da->upcall_prog)  {
    do_upcall (da->upcall_prog, da->upcall_proc,
	       da->upcall_args.base (), da->upcall_args.size (),
	       wrap (this, &vnode_impl::debruijn_upcall_done, da, res, sbp));
    
  } else {
    sbp->reply (&res);
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
			    chordID s, route r, chordstat err)
{
  if (err) {
    chord_nodelistres res (CHORD_RPCFAILURE);
    sbp->reply (&res);
  } else {
    chord_nodelistres res (CHORD_OK);
    res.resok->nlist.setsize (r.size ());
    for (unsigned int i = 0; i < r.size (); i++) {
      bool ok = locations->get_node (r[i], &res.resok->nlist[i]);
      assert (ok);
    }
    sbp->reply (&res);
  }
}

void
vnode_impl::stop (void)
{
  stabilizer->stop ();
}
  
