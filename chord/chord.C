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

#define PNODE 
//#define TOES 1

vnode::vnode (ptr<locationtable> _locations, ptr<chord> _chordnode,
	      chordID _myID, int _vnode, int server_sel_mode) :
  locations (_locations),
  myindex (_vnode),
  myID (_myID), 
  chordnode (_chordnode),
  server_selection_mode (server_sel_mode)
{
  warnx << gettime () << " myID is " << myID << "\n";
  nout_continuous = 0;
  nout_backoff = 0;

  fingers = New refcounted<finger_table> (locations, myID);
  successors = New refcounted<succ_list> (locations, myID);
  toes = New refcounted<toe_table> (locations, successors);

  stable_fingers = false;
  stable_fingers2 = false;
  stable_succlist = false;
  stable_succlist2 = false;
  stable = false;
  locations->incvnodes ();

  predecessor.n = myID;
  predecessor.alive = true;

  nnodes = 0;
  ngetsuccessor = 0;
  ngetpredecessor = 0;
  nfindsuccessor = 0;
  nslowfinger = 0;
  nfastfinger = 0;
  nhops = 0;
  nmaxhops = 0;
  nfindpredecessor = 0;
  nfindsuccessorrestart = 0;
  nfindpredecessorrestart = 0;
  nnotify = 0;
  nalert = 0;
  ntestrange = 0;
  ngetfingers = 0;
  nchallenge = 0;
  ndogetsuccessor = 0;
  ndogetpredecessor = 0;
  ndofindclosestpred = 0;
  ndonotify = 0;
  ndoalert = 0;
  ndotestrange = 0;
  ndogetfingers = 0;
  ndochallenge = 0;
}

vnode::~vnode ()
{
  warnx << "vnode: destroyed\n";
  exit (0);
}

chordID
vnode::my_succ () 
{
  if (fingers->succ_alive ()) return fingers->succ ();
  return successors->first_succ ();
}

int
vnode::countrefs (chordID &x)
{
  int n = fingers->countrefs (x);
  n += successors->countrefs (x);
  if (predecessor.alive && (x == predecessor.n)) n++;
  return n;
}

void
vnode::stats ()
{
  warnx << "VIRTUAL NODE STATS " << myID << " stable? " << isstable () << "\n";
  warnx << "# estimated node in ring " << nnodes << "\n";
  warnx << "continuous_timer " << continuous_timer << " backoff " 
	<< backoff_timer << "\n";
  
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
  warnx << "# fast check in stabilize of finger table " << nfastfinger << "\n";
  warnx << "# slow findsuccessor in stabilize of finger table " << 
    nslowfinger << "\n";
  warnx << "# findpredecessor calls " << nfindpredecessor << "\n";
  warnx << "# findsuccessorrestart calls " << nfindsuccessorrestart << "\n";
  warnx << "# findpredecessorrestart calls " << nfindpredecessorrestart << "\n";
  warnx << "# rangandtest calls " << ntestrange << "\n";
  warnx << "# notify calls " << nnotify << "\n";  
  warnx << "# alert calls " << nalert << "\n";  
  warnx << "# getfingers calls " << ngetfingers << "\n";
  warnx << "# challenge calls " << nchallenge << "\n";
}

void
vnode::print ()
{
  warnx << "======== " << myID << "====\n";
  fingers->print ();
  successors->print ();

  warnx << "pred : " << predecessor.n << "\n";
#ifdef TOES
  warnx << "------------- toes ----------------------------------\n";
  toes->dump ();
#endif /*TOES*/
  warnx << "=====================================================\n";

}


chordID
vnode::lookup_closestsucc (chordID &x)
{
#ifdef PNODE
  chordID f = fingers->closestsuccfinger (x);
  chordID s = successors->closest_succ (x);
  if (between (x, s, f)) 
    return f;
  else 
    return s;
#else
  chordID s = locations->closestsuccloc (x);
#endif
  return s;
}

chordID
vnode::lookup_closestpred (chordID &x)
{
#ifdef PNODE
  chordID f = fingers->closestpredfinger (x);
  chordID s = successors->closest_pred (x);
  if (between (f, x, s)) 
    return s;
  else
    return f;
#else
  return locations->closestpredloc (x);
#endif

}

void
vnode::join (cbjoin_t cb)
{
  chordID n;

  if (!locations->lookup_anyloc (myID, &n)) {
    (*cb)(NULL, CHORD_ERRNOENT);
  } else {
    fingers->updatefinger (n);
    find_successor (myID, wrap (mkref (this), &vnode::join_getsucc_cb, cb));
  }
}

void 
vnode::join_getsucc_cb (cbjoin_t cb, chordID s, route r, chordstat status)
{
  if (status) {
    warnx << "join_getsucc_cb: " << myID << " " << status << "\n";
    join (cb);  // try again
  } else {
    challenge (s, wrap (mkref (this), &vnode::join_getsucc_ok, cb));
  }
}

void
vnode::join_getsucc_ok (cbjoin_t cb, chordID s, bool ok, chordstat status)
{
  if ((status == CHORD_OK) && ok) {
    fingers->updatefinger (s);
    stabilize ();
    (*cb) (this, CHORD_OK);
  } else if (status == CHORD_OK) {
    warnx << "join_getsucc_ok: " << myID 
	  << " join failed, because succ is not authentic\n";
  } else {
    join (cb);  // try again
  }
}

void
vnode::doget_successor (svccb *sbp)
{
  ndogetsuccessor++;
  if (fingers->succ_alive ()) {
    chordID s = fingers->succ ();
    chord_noderes res(CHORD_OK);
    res.resok->node = s;
    res.resok->r = locations->getaddress (s);
    sbp->reply (&res);
  } else {
    sbp->replyref (chordstat (CHORD_ERRNOENT));
  }
  warnt("CHORD: doget_successor_reply");
}

void
vnode::doget_predecessor (svccb *sbp)
{
  ndogetpredecessor++;
  if (predecessor.alive) {
    chordID p = predecessor.n;
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
  chordID succ = fingers->succ ();

  if (fingers->succ_alive () && 
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
vnode::donotify_cb (chordID p, bool ok, chordstat status)

{
  if ((status == CHORD_OK) && ok) {
    if ((!predecessor.alive) || between (predecessor.n, myID, p)) {
      net_address r = locations->getaddress (p);
      // warnx << "donotify_cb: updated predecessor: new pred is " << p << "\n";
      locations->changenode (&predecessor, p, r);
      fingers->updatefinger (predecessor.n);
      get_fingers (predecessor.n); // XXX perhaps do this only once after join
    }
  } else if (status == CHORD_OK) {
      warnx << "donotify_cb: couldn't authenticate " << p << "\n";
  }
}

void
vnode::donotify (svccb *sbp, chord_nodearg *na)
{
  ndonotify++;
  if ((!predecessor.alive) || between (predecessor.n, myID, na->n.x)) {
      // warnx << "donotify: challenge: " << na->n.x << "\n";
      locations->cacheloc (na->n.x, na->n.r);
      challenge (na->n.x, wrap (mkref (this), &vnode::donotify_cb));
  }
  sbp->replyref (chordstat (CHORD_OK));
}

void
vnode::doalert (svccb *sbp, chord_nodearg *na)
{
  ndoalert++;
  warnt("CHORD: doalert");
  if (locations->getlocation (na->n.x) != NULL) {
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
    chordnode->deletefingers (x);
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
  res.resok->pred.alive = predecessor.alive;
  if (predecessor.alive) {
    res.resok->pred.x = predecessor.n;
    res.resok->pred.r = locations->getaddress (predecessor.n);
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
    location *l = locations->getlocation (t[i]);
    res.resok->toes[i].x = t[i];
    res.resok->toes[i].r = l->addr;
    res.resok->toes[i].a_lat = (long)(l->a_lat * 100);
    res.resok->toes[i].a_var = (long)(l->a_var * 100);
    res.resok->toes[i].nrpc = l->nrpc;
    res.resok->toes[i].alive = true;
  }
  
  warnt ("CHORD: dogettoes_reply");
  sbp->reply (&res);
}

void
vnode::deletefingers (chordID &x) 
{
 fingers->deletefinger (x);
 successors->delete_succ (x);

 if (predecessor.alive && (x == predecessor.n)) {
   locations->deleteloc (predecessor.n);
   predecessor.alive = false;
 }
}
