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

  fingers = New refcounted<finger_table> (mkref (this), locations, myID);
  successors = New refcounted<succ_list> (mkref (this), locations, myID);
  toes = New refcounted<toe_table> (locations, myID);
  stabilizer = New refcounted<stabilize_manager> (myID);

  stabilizer->register_client (successors);
  stabilizer->register_client (mkref (this));
  stabilizer->register_client (fingers);
  stabilizer->register_client (toes);
    
  locations->incvnodes ();

  predecessor = myID;

  ngetsuccessor = 0;
  ngetpredecessor = 0;
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
  ndotestrange = 0;
  ndogetfingers = 0;
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
  warnx << "# notify requests " << ndonotify << "\n";  
  warnx << "# alert requests " << ndoalert << "\n";  
  warnx << "# testrange requests " << ndotestrange << "\n";  
  warnx << "# getfingers requests " << ndogetfingers << "\n";
  warnx << "# dochallenge requests " << ndochallenge << "\n";

  warnx << "# getsuccesor calls " << ngetsuccessor << "\n";
  warnx << "# getpredecessor calls " << ngetpredecessor << "\n";
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

  warnx << "pred : " << predecessor << "\n";
#ifdef TOES
  warnx << "------------- toes ----------------------------------\n";
  toes->dump ();
#endif /*TOES*/
  warnx << "=====================================================\n";

}


chordID
vnode::lookup_closestsucc (chordID &x)
{
  chordID s = locations->closestsuccloc (x);
  return s;
}

chordID
vnode::lookup_closestpred (chordID &x)
{
  chordID s = locations->closestpredloc (x);
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
    warnx << myID << ": joining at " << n << "\n";
    find_successor (myID, wrap (mkref (this), &vnode::join_getsucc_cb, cb));
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
  res.resok->node = s;
  res.resok->r = locations->getaddress (s);
  sbp->reply (&res);

  warnt("CHORD: doget_successor_reply");
}

void
vnode::doget_predecessor (svccb *sbp)
{
  ndogetpredecessor++;
  if (locations->alive (predecessor)) {
    chordID p = predecessor;
    chord_noderes res(CHORD_OK);
    res.resok->node = p;
    res.resok->r = locations->getaddress (p);
    sbp->reply (&res);
  } else {
    sbp->replyref (chordstat (CHORD_ERRNOENT));
  }
}

void
vnode::dotestrange_findclosestpred (svccb *sbp, chord_testandfindarg *fa) 
{
  ndotestrange++;
  chordID x = fa->x;
  chord_testandfindres *res;
  chordID succ = successors->succ ();

  if (locations->alive (succ) && 
      betweenrightincl(myID, succ, x) ) {
    res = New chord_testandfindres (CHORD_INRANGE);
    warnt("CHORD: testandfind_inrangereply");

    res->inres->x = succ;
    res->inres->r = locations->getaddress (succ);
    sbp->reply(res);
    delete res;
  } else {
    res = New chord_testandfindres (CHORD_NOTINRANGE);
    chordID p = lookup_closestpred (fa->x);
    res->noderes->node = p;
    res->noderes->r = locations->getaddress (p);
    // warnx << "dotestrange_findclosestpred: " << myID << " closest pred of " 
    //      << fa->x << " is " << p << "\n";
    warnt("CHORD: testandfind_notinrangereply");
    sbp->reply(res);
    delete res;
  }
  
}

void
vnode::dofindclosestpred (svccb *sbp, chord_findarg *fa)
{
  chord_noderes res(CHORD_OK);
  chordID p = lookup_closestpred (fa->x);
  ndofindclosestpred++;
  res.resok->node = p;
  res.resok->r = locations->getaddress (p);
  warnt("CHORD: dofindclosestpred_reply");
  sbp->reply (&res);
}

void
vnode::updatepred_cb (chordID p, bool ok, chordstat status)
{
  if ((status == CHORD_OK) && ok) {
    if (predecessor == myID ||
	!locations->alive (predecessor) ||
	between (predecessor, myID, p)) {
      predecessor = p;
      get_fingers (predecessor); // XXX perhaps do this only once after join
    }
  }
  else if (status == CHORD_RPCFAILURE) {
    warnx << myID << ": updatepred_cb: couldn't authenticate " << p << "\n";
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
  if (predecessor == myID ||
      !locations->alive (predecessor) ||
      between (predecessor, myID, na->n.x)) {
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
  chord_getfingersres res(CHORD_OK);
  ndogetfingers++;
  fingers->fill_getfingersres (&res);
  warnt("CHORD: dogetfingers_reply");
  sbp->reply (&res);
}


void
vnode::dogetfingers_ext (svccb *sbp)
{
  chord_getfingers_ext_res res(CHORD_OK);
  ndogetfingers_ext++;

  fingers->fill_getfingersresext (&res);
  res.resok->pred.alive = locations->alive (predecessor);
  if (locations->alive (predecessor)) {
    res.resok->pred.x = predecessor;
    res.resok->pred.r = locations->getaddress (predecessor);
  }

  warnt("CHORD: dogetfingers_reply");
  sbp->reply (&res);
}

void
vnode::dogetsucc_ext (svccb *sbp)
{
  chord_getsucc_ext_res res(CHORD_OK);
  ndogetsucc_ext++;
  successors->fill_getsuccres (&res);
  warnt("CHORD: dogetsucc_reply");
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
  chord_gettoes_res res (CHORD_OK);
  vec<chordID> t = toes->get_toes (ta->level);
  
  ndogettoes++;
  res.resok->toes.setsize (t.size ());
  for (unsigned int i = 0; i < t.size (); i++) {
    locations->fill_getnodeext (res.resok->toes[i], t[i]);
  }
  
  warnt ("CHORD: dogettoes_reply");
  sbp->reply (&res);
}

void
vnode::stop (void)
{
  stabilizer->stop ();
}
  

void
vnode::stabilize_pred ()
{
  if (!locations->alive (predecessor)) {
    predecessor = locations->closestpredloc (myID);
  }
  nout_continuous++;
  get_successor (predecessor,
		 wrap (this, &vnode::stabilize_getsucc_cb,
		       predecessor));
}

void
vnode::stabilize_getsucc_cb (chordID pred, 
			     chordID s, net_address r, chordstat status)
{
  // receive successor from my predecessor; in stable case it is me
  nout_continuous--;
  if (status) {
    warnx << myID << ": stabilize_getpred_cb: " << pred 
	  << " failure " << status << "\n";
  } else {
    // maybe we're not stable. try this guy's successor as our new pred
    if (s != myID) {
      locations->cacheloc (s, r,
			   wrap (this, &vnode::updatepred_cb));
    }
  }
}

bool
vnode::isstable ()
{
  return locations->alive (predecessor);
}
