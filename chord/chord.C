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
//#define TOES 1

vnode::vnode (ptr<locationtable> _locations, ptr<chord> _chordnode,
	      chordID _myID, int _vnode, int server_sel_mode) :
  myindex (_vnode),
  myID (_myID), 
  chordnode (_chordnode),
  locations (_locations),
  server_selection_mode (server_sel_mode)
{
  warnx << gettime () << " myID is " << myID << "\n";
#ifdef FINGERS
  fingers = New refcounted<finger_table> (mkref (this), locations, myID);
#else
  dutch = New refcounted<debruijn> (mkref (this), locations, myID);
#endif
  successors = New refcounted<succ_list> (mkref (this), locations, myID);
  toes = New refcounted<toe_table> (locations, myID);
  stabilizer = New refcounted<stabilize_manager> (myID);

  stabilizer->register_client (successors);
  stabilizer->register_client (mkref (this));
#ifdef FINGERS
  stabilizer->register_client (fingers);
#else
  stabilizer->register_client (dutch);
#endif
  stabilizer->register_client (toes);
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
#ifdef FINGERS
  fingers->stats ();
#endif
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
#ifdef FINGERS
  fingers->print ();
#else
  dutch->print ();
#endif
  successors->print ();

  warnx << "pred : " << my_pred () << "\n";
#ifdef TOES
  warnx << "------------- toes ----------------------------------\n";
  toes->dump ();
#endif /*TOES*/
  warnx << "=====================================================\n";

}

#define LOCLOOKUP 1

chordID
vnode::lookup_closestsucc (const chordID &x)
{
#ifdef LOCLOOKUP
  chordID s = locations->closestsuccloc (x);
#else
#ifdef FINGERS
  chordID s = fingers->closestsucc (x);
#else
  chordID s = dutch->closestsucc (x);
#endif
#endif
  return s;
}

chordID
vnode::lookup_closestpred (const chordID &x)
{
#if LOCLOOKUP
  chordID s = locations->closestpredloc (x);
#else
#ifdef FINGERS
  chordID s = fingers->closestpred (x);
#else
  chordID s = dutch->closestpred (x);
#endif
#endif
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
    (*cb) (this, CHORD_OK);
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

  warnt("CHORD: doget_successor_reply");
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
vnode::do_upcall (int upcall_prog, int upcall_proc,
		  void *uc_args, int uc_args_len,
		  cbupcalldone_t done_cb)

{
  cbupcall_t cb = upcall_table[upcall_prog];
  assert (cb);
  
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
  (*cb)(upcall_proc, (void *)unmarshalled_args, done_cb);
}

void
vnode::dotestrange_findclosestpred (svccb *sbp, chord_testandfindarg *fa) 
{
  ndotestrange++;
  chordID x = fa->x;
  chordID succ = successors->succ ();

  chord_testandfindres *res = New chord_testandfindres ();  
  if (locations->alive (succ) && 
      betweenrightincl(myID, succ, x) ) {
    warnt("CHORD: testandfind_inrangereply");
    res->set_status (CHORD_INRANGE);
    res->inrange->n.x = succ;
    res->inrange->n.r = locations->getaddress (succ);
  } else {
    res->set_status (CHORD_NOTINRANGE);
    chordID p = lookup_closestpred (fa->x);
    res->notinrange->n.x = p;
    res->notinrange->n.r = locations->getaddress (p);
    warnt("CHORD: testandfind_notinrangereply");
  }

  if (fa->upcall_prog)  {
    do_upcall (fa->upcall_prog, fa->upcall_proc,
	       fa->upcall_args.base (), fa->upcall_args.size (),
	       wrap (this, &vnode::chord_upcall_done, fa, res, sbp));

  } else {
    if (res->status == CHORD_INRANGE) res->inrange->stop = false;
    else res->inrange->stop = false;

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
  
  if (res->status == CHORD_INRANGE) res->inrange->stop = stop;
  else res->notinrange->stop = stop;

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
  warnt("CHORD: dofindclosestpred_reply");
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
  warnt("CHORD: doalert");
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
#ifdef FINGERS
  chord_nodelistres res(CHORD_OK);
  ndogetfingers++;
  fingers->fill_getfingersres (&res);
  warnt("CHORD: dogetfingers_reply");
  sbp->reply (&res);
#else
  chord_nodelistres res(CHORD_ERRNOENT);
  sbp->reply (&res);
#endif
}


void
vnode::dogetfingers_ext (svccb *sbp)
{
#ifdef FINGERS
  chord_nodelistextres res(CHORD_OK);
  ndogetfingers_ext++;

  fingers->fill_getfingersresext (&res);

  warnt("CHORD: dogetfingers_reply");
  sbp->reply (&res);
#else
  chord_nodelistres res(CHORD_ERRNOENT);
  sbp->reply (&res);
#endif
}

void
vnode::dogetsucc_ext (svccb *sbp)
{
  chord_nodelistextres res(CHORD_OK);
  ndogetsucc_ext++;
  successors->fill_getsuccres (&res);
  warnt("CHORD: dogetsucc_reply");
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
  
  warnt ("CHORD: dogettoes_reply");
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
  chordID pred = my_pred ();

  //  warnx << myID << " dodebruijn: pred " << pred << " x " << da->x << " d " 
  // << da->d << " between " << betweenrightincl (pred, myID, da->d) << "\n";

  if (betweenrightincl (pred, myID, da->x)) {
    res = New chord_debruijnres (CHORD_INRANGE);
    warnt("CHORD: debruijn_inrangereply");
    res->inres->x = myID;
    res->inres->r = locations->getaddress (myID);
    sbp->reply(res);
    delete res;
  } else {
    res = New chord_debruijnres (CHORD_NOTINRANGE);
    if (betweenrightincl (pred, myID, da->d)) {
      chordID nd = lookup_closestsucc (doubleID(myID, LOGBASE));
      res->noderes->node.x = nd;
      res->noderes->node.r = locations->getaddress (nd);
      res->noderes->d = doubleID(da->d, LOGBASE);
    } else {
      res->noderes->node.x = lookup_closestsucc (da->d); // pred
      assert (res->noderes->node.x != myID);
      res->noderes->node.r = locations->getaddress (res->noderes->node.x);
      res->noderes->d = da->d;
    }
    sbp->reply(res);
    delete res;
  }
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
