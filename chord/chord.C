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
#include <chord.h>
#include <qhash.h>

const int chord::max_vnodes = 1024;

const int CHORD_LOOKUP_FINGERLIKE (0);
const int CHORD_LOOKUP_LOCTABLE (1);
const int CHORD_LOOKUP_PROXIMITY (2);

vnode::vnode (ptr<locationtable> _locations, ptr<fingerlike> stab,
	      ptr<route_factory> f,
	      ptr<chord> _chordnode,
	      chordID _myID, int _vnode, int server_sel_mode,
	      int l_mode) :
  myindex (_vnode),
  myID (_myID), 
  chordnode (_chordnode),
  locations (_locations),
  factory (f),
  server_selection_mode (server_sel_mode),
  lookup_mode (l_mode)
{
  warnx << gettime () << " myID is " << myID << "\n";

  fingers = stab;
  fingers->init (mkref(this), locations, myID);

  successors = New refcounted<succ_list> (mkref(this), locations, myID);
  stabilizer = New refcounted<stabilize_manager> (myID);

  stabilizer->register_client (successors);
  stabilizer->register_client (mkref (this));
  stabilizer->register_client (fingers);

  if (lookup_mode == CHORD_LOOKUP_PROXIMITY) {
    toes = New refcounted<toe_table> ();
    toes->init (mkref(this), locations, myID);
    stabilizer->register_client (toes);
  }

#ifndef PNODE
  // If vnode's share a locationtable, then we don't need to register this
  // more than once.
  if (myindex == 0)
#endif /* PNODE */    
    stabilizer->register_client (locations);
    
  locations->incvnodes ();

  locations->pinpred (myID);
  locations->pinsucc (myID);

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
  
  nout_continuous = 0;
}

vnode::~vnode ()
{
  warnx << myID << ": vnode: destroyed\n";
  exit (0);
}

chordID
vnode::my_pred() 
{
  return locations->closestpredloc (myID);
}

chordID
vnode::my_succ () 
{
  return successors->succ ();
}

void
vnode::stats ()
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
#ifdef PNODE
  // Each node has its own location table in this case.
  locations->stats ();
#endif /* PNODE */  
}

void
vnode::print ()
{
  warnx << "======== " << myID << "====\n";
  fingers->print ();
  successors->print ();

  warnx << "pred : " << my_pred () << "\n";
  if (lookup_mode == CHORD_LOOKUP_PROXIMITY) {
    warnx << "------------- toes ----------------------------------\n";
    toes->dump ();
  }
  warnx << "=====================================================\n";

}

chordID
vnode::lookup_closestsucc (const chordID &x)
{
  chordID s;

  if (lookup_mode == CHORD_LOOKUP_PROXIMITY)
    s = toes->closestsucc (x);
  else if (lookup_mode == CHORD_LOOKUP_FINGERLIKE)
    s = fingers->closestsucc (x);
  else
    s = locations->closestsuccloc (x);

  return s;
}

chordID
vnode::lookup_closestpred (const chordID &x)
{
  chordID s;

  if (lookup_mode == CHORD_LOOKUP_PROXIMITY)
    s = toes->closestpred (x);
  else if (lookup_mode == CHORD_LOOKUP_FINGERLIKE)
    s = fingers->closestpred (x);
  else 
    s = locations->closestpredloc (x);

  return s;
}

void
vnode::stabilize (void)
{
  stabilizer->start ();
}

void
vnode::join (cbjoin_t cb)
{
  chordID n;

  if (!locations->lookup_anyloc (myID, &n)) {
    warnx << myID << ": couldn't lookup anyloc for join\n";
    locations->stats ();
    (*cb) (NULL, CHORD_ERRNOENT);
  } else {
    chordID s = myID + 1;
    find_successor (s, wrap (mkref (this), &vnode::join_getsucc_cb, cb));
  }
}

void 
vnode::join_getsucc_cb (cbjoin_t cb, chordID s, route r, chordstat status)
{
  if (status) {
    warnx << myID << ": join_getsucc_cb: " << status << "\n";
    join (cb);  // try again
  } else {
    stabilize ();
    notify (s, myID);
    (*cb) (mkref(this), CHORD_OK);
  }
}

void
vnode::doget_successor (svccb *sbp)
{
  ndogetsuccessor++;
  
  chordID s = successors->succ ();
  chord_noderes res(CHORD_OK);
  res.resok->x = s;
  res.resok->r = locations->getaddress (s);
  sbp->reply (&res);
}

void
vnode::doget_predecessor (svccb *sbp)
{
  ndogetpredecessor++;
  chordID p = my_pred ();
  chord_noderes res(CHORD_OK);
  res.resok->x = p;
  res.resok->r = locations->getaddress (p);
  sbp->reply (&res);
}

void
vnode::do_upcall_cb (char *a, cbupcalldone_t done_cb, bool v)
{
  delete[] a;
  done_cb (v);
}

void
vnode::do_upcall (int upcall_prog, int upcall_proc,
		  void *uc_args, int uc_args_len,
		  cbupcalldone_t done_cb)

{
  upcall_record *uc = upcall_table[upcall_prog];
  if (!uc) { 
    warn << "upcall not registered\n";
    done_cb (false);
    return;
  }

  rpc_program *prog;
  chordnode->get_program (upcall_prog, &prog);
  assert (prog);
  
  xdrmem x ((char *)uc_args, uc_args_len, XDR_DECODE);
  xdrproc_t proc = prog->tbl[upcall_proc].xdr_arg;
  assert (proc);
  
  char *unmarshalled_args = New char[uc_args_len];
  bzero (unmarshalled_args, uc_args_len);
  if (!proc (x.xdrp (), unmarshalled_args))
    fatal << "upcall: error unmarshalling arguments\n";
  
  //run the upcall. It returns a pointer to its result and a length in the cb
  cbupcall_t cb = uc->cb;
  (*cb)(upcall_proc, (void *)unmarshalled_args,
	wrap (this, &vnode::do_upcall_cb, unmarshalled_args, done_cb));

}

void
vnode::dotestrange_findclosestpred (svccb *sbp, chord_testandfindarg *fa) 
{
  ndotestrange++;
  chordID x = fa->x;
  chordID succ = my_succ ();

  chord_testandfindres *res = New chord_testandfindres ();  
  if (betweenrightincl(myID, succ, x) ) {
    res->set_status (CHORD_INRANGE);
    res->inrange->n.x = succ;
    res->inrange->n.r = locations->getaddress (succ);
  } else {
    res->set_status (CHORD_NOTINRANGE);
    chordID p = lookup_closestpred (fa->x);
    res->notinrange->n.x = p;
    res->notinrange->n.r = locations->getaddress (p);
  }

  if (fa->upcall_prog)  {
    do_upcall (fa->upcall_prog, fa->upcall_proc,
	       fa->upcall_args.base (), fa->upcall_args.size (),
	       wrap (this, &vnode::chord_upcall_done, fa, res, sbp));

  } else {
    sbp->reply(res);
    delete res;
  }
}

void
vnode::chord_upcall_done (chord_testandfindarg *fa,
			  chord_testandfindres *res,
			  svccb *sbp,
			  bool stop)
{
  
  if (stop) res->set_status (CHORD_STOP);
  sbp->reply (res);
  delete res;
}

void
vnode::dofindclosestpred (svccb *sbp, chord_findarg *fa)
{
  chord_noderes res(CHORD_OK);
  chordID p = lookup_closestpred (fa->x);
  ndofindclosestpred++;
  res.resok->x = p;
  res.resok->r = locations->getaddress (p);
  sbp->reply (&res);
}

void
vnode::updatepred_cb (chordID p, bool ok, chordstat status)
{
  if (status == CHORD_RPCFAILURE) {
    warnx << myID << ": updatepred_cb: couldn't authenticate " << p << "\n";
  } else if ((status == CHORD_OK) & ok) {
    get_fingers (p);
  }
  // If !ok but status == CHORD_OK, then there's probably another
  // outstanding call somewhere that is already testing this same
  // node. We can ignore the failure and wait until the outstanding
  // challenge returns. If they all fail, then we'll never get called
  // and that's okay too.
}

void
vnode::donotify (svccb *sbp, chord_nodearg *na)
{
  ndonotify++;
  if (my_pred () == myID || between (my_pred (), myID, na->n.x)) {
    locations->cacheloc (na->n.x, na->n.r,
			 wrap (mkref (this), &vnode::updatepred_cb));
  }
  sbp->replyref (chordstat (CHORD_OK));
}

void
vnode::doalert (svccb *sbp, chord_nodearg *na)
{
  ndoalert++;
  if (locations->cached (na->n.x)) {
    // check whether we cannot reach x either
    get_successor (na->n.x, wrap (mkref (this), &vnode::doalert_cb, sbp, 
				  na->n.x));
  } else {
    sbp->replyref (chordstat (CHORD_UNKNOWNNODE));
  }
}

void
vnode::doalert_cb (svccb *sbp, chordID x, chordID s, net_address r, 
		   chordstat stat)
{
  if ((stat == CHORD_OK) || (stat == CHORD_ERRNOENT)) {
    warnx << "doalert_cb: " << x << " is alive\n";
    sbp->replyref (chordstat (CHORD_OK));
  } else {
    warnx << "doalert_cb: " << x << " is indeed not alive\n";
    // doRPCcb has already killed this node for us.
    sbp->replyref (chordstat (CHORD_UNKNOWNNODE));
  }
}

void
vnode::dogetfingers (svccb *sbp)
{
  chord_nodelistres res(CHORD_OK);
  ndogetfingers++;
  fingers->fill_nodelistres (&res);
  sbp->reply (&res);
}


void
vnode::dogetfingers_ext (svccb *sbp)
{
  chord_nodelistextres res(CHORD_OK);
  ndogetfingers_ext++;

  fingers->fill_nodelistresext (&res);

  sbp->reply (&res);
}

void
vnode::dogetsucc_ext (svccb *sbp)
{
  chord_nodelistextres res(CHORD_OK);
  ndogetsucc_ext++;
  successors->fill_nodelistresext (&res);
  sbp->reply (&res);
}

void
vnode::dogetpred_ext (svccb *sbp)
{
  ndogetpred_ext++;
  chord_nodeextres res(CHORD_OK);
  res.resok->alive = true;
  locations->fill_getnodeext (*res.resok, my_pred ());
  sbp->reply (&res);
}

void
vnode::dochallenge (svccb *sbp, chord_challengearg *ca)
{
  chord_challengeres res(CHORD_OK);
  ndochallenge++;
  res.resok->index = myindex;
  res.resok->challenge = ca->challenge;
  sbp->reply (&res);
}

void
vnode::dogettoes (svccb *sbp)
{
  chord_gettoes_arg *ta = 
    sbp->template getarg<chord_gettoes_arg> ();
  chord_nodelistextres res (CHORD_OK);
  vec<chordID> t = toes->get_toes (ta->level);
  
  ndogettoes++;
  res.resok->nlist.setsize (t.size ());
  for (unsigned int i = 0; i < t.size (); i++) {
    locations->fill_getnodeext (res.resok->nlist[i], t[i]);
  }
  
  sbp->reply (&res);
}

void
vnode::dogetsucclist (svccb *sbp)
{
  chord_nodelistres res (CHORD_OK);
  ndogetsucclist++;
  
  int curnsucc = successors->num_succ ();
  chordID cursucc = myID;
  res.resok->nlist.setsize (curnsucc + 1);
  for (int i = 0; i <= curnsucc; i++) {
    res.resok->nlist[i].x = cursucc;
    res.resok->nlist[i].r = locations->getaddress (cursucc);
    cursucc = locations->closestsuccloc (cursucc + 1);
  }
  sbp->reply (&res);
}

void
vnode::dodebruijn (svccb *sbp, chord_debruijnarg *da)
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
    res->inres->node.x = succ;
    res->inres->node.r = locations->getaddress (succ);
  } else {
    res->set_status (CHORD_NOTINRANGE);
    if (betweenrightincl (myID, succ, da->i)) {
      // ptr<debruijn> d = dynamic_cast< ptr<debruijn> >(fingers);
      // assert (d);  // XXXX return error
      // chordID nd =  d->debruijnprt (); 
      chordID nd = lookup_closestpred (doubleID (myID, logbase));
      res->noderes->node.x = nd;
      res->noderes->node.r = locations->getaddress (nd);
      res->noderes->i = doubleID (da->i, logbase);
      res->noderes->i = res->noderes->i | topbits (logbase, da->k);
      res->noderes->k = shifttopbitout (logbase, da->k);
    } else {
      res->noderes->node.x = lookup_closestpred (da->i); // succ
      assert (res->noderes->node.x != myID);
      res->noderes->node.r = locations->getaddress (res->noderes->node.x);
      res->noderes->i = da->i;
      res->noderes->k = da->k;
    }
  }

  if (da->upcall_prog)  {
    do_upcall (da->upcall_prog, da->upcall_proc,
	       da->upcall_args.base (), da->upcall_args.size (),
	       wrap (this, &vnode::debruijn_upcall_done, da, res, sbp));
    
  } else {
    sbp->reply (res);
    delete res;
  }
}

void
vnode::debruijn_upcall_done (chord_debruijnarg *da,
			     chord_debruijnres *res,
			     svccb *sbp,
			     bool stop)
{
  
  if (stop) res->set_status (CHORD_STOP);
  sbp->reply (res);
  delete res;
}

void
vnode::dofindroute (svccb *sbp, chord_findarg *fa)
{
  find_route (fa->x, wrap (this, &vnode::dofindroute_cb, sbp));
}

void
vnode::dofindroute_cb (svccb *sbp, chordID s, route r, chordstat err)
{
  if (err) {
    chord_nodelistres res (CHORD_RPCFAILURE);
    sbp->reply (&res);
  } else {
    chord_nodelistres res (CHORD_OK);
    res.resok->nlist.setsize (r.size ());
    for (unsigned int i = 0; i < r.size (); i++) {
      res.resok->nlist[i].x = r[i];
      res.resok->nlist[i].r = locations->getaddress (r[i]);
    }
    sbp->reply (&res);
  }
}

void
vnode::stop (void)
{
  stabilizer->stop ();
}
  

void
vnode::stabilize_pred ()
{
  chordID p = my_pred ();

  assert (nout_continuous == 0);

  nout_continuous++;
  get_successor (p, wrap (this, &vnode::stabilize_getsucc_cb, p));
}

void
vnode::stabilize_getsucc_cb (chordID pred, 
			     chordID s, net_address r, chordstat status)
{
  // receive successor from my predecessor; in stable case it is me
  nout_continuous--;
  if (status) {
    warnx << myID << ": stabilize_pred: " << pred 
	  << " failure " << status << "\n";
  } else {
    // maybe we're not stable. insert this guy's successor in
    // location table; maybe he is our predecessor.
    if (s != myID) { 
      locations->cacheloc (s, r,
			   wrap (this, &vnode::updatepred_cb));
    } else {
      last_pred = pred;
    }
  }
}


void 
vnode::fill_nodelistresext (chord_nodelistextres *res)
{
}

void 
vnode::fill_nodelistres (chord_nodelistres *res)
{
  
}


