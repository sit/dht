/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#include <assert.h>
#include <chord.h>
#include <qhash.h>

#define PNODE
#define STOPSTABILIZE		// for testing purposes

vnode::vnode (ptr<locationtable> _locations, ptr<chord> _chordnode,
	      chordID _myID) :
  locations (_locations),
  myID (_myID), 
  chordnode (_chordnode)
{
  warnx << gettime () << " myID is " << myID << "\n";
  finger_table[0].start = finger_table[0].first.n = myID;
  finger_table[0].first.alive = true;
  locations->increfcnt (myID);
  succlist[0].n = myID;
  succlist[0].alive = true;
  nsucc = 0;
  stable_fingers = false;
  stable_fingers2 = false;
  stable_succlist = false;
  stable_succlist2 = false;
  stable = false;
  locations->incvnodes ();
  locations->increfcnt (myID);
  for (int i = 1; i <= NSUCC; i++) {
    succlist[i].alive = false;
  }
  for (int i = 1; i <= NBIT; i++) {
    locations->increfcnt (myID);
    finger_table[i].start = successorID(myID, i-1);
    finger_table[i].first.n = myID;
    finger_table[i].first.alive = true;
  }
  locations->increfcnt (myID);
  predecessor.n = myID;
  predecessor.alive = true;

  nnodes = 0;
  ngetsuccessor = 0;
  ngetpredecessor = 0;
  nfindsuccessor = 0;
  nhops = 0;
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
}

vnode::~vnode ()
{
  warnx << "vnode: destroyed\n";
  exit (0);
}

int
vnode::countrefs (chordID &x)
{
  int n = 0;
  for (int i = 0; i <= NBIT; i++) {
    if (finger_table[i].first.alive && (x == finger_table[i].first.n))
      n++;
    if (succlist[i].alive && (x == succlist[i].n))
      n++;
  }
  if (predecessor.alive && (x == predecessor.n)) n++;
  return n;
}

void 
vnode::updatefingers (chordID &x, net_address &r)
{
  for (int i = 1; i <= NBIT; i++) {
    if (between (finger_table[i].start, finger_table[i].first.n, x)) {
      assert (finger_table[i].first.n != x);
      locations->changenode (&finger_table[i].first, x, r);
    }
  }
}

void
vnode::replacefinger (node *n)
{
#ifdef PNODE
  n->n = findsuccfinger (n->n);
#else
  n->n = locations->findsuccloc (n->n);
#endif
  n->alive = true;
  locations->increfcnt (n->n);
}

void 
vnode::deletefingers (chordID &x)
{
  if (x == myID) return;

  for (int i = 1; i <= NBIT; i++) {
    if (finger_table[i].first.alive && (x == finger_table[i].first.n)) {
      locations->deleteloc (finger_table[i].first.n);
      finger_table[i].first.alive = false;
    }
  }
  for (int i = 1; i <= NSUCC; i++) {
    if (succlist[i].alive && (x == succlist[i].n)) {
      locations->deleteloc (succlist[i].n);
      succlist[i].alive = false;
    }
  }
  if (predecessor.alive && (x == predecessor.n)) {
    locations->deleteloc (predecessor.n);
    predecessor.alive = false;
  }
}

void
vnode::stats ()
{
  warnx << "VIRTUAL NODE STATS " << myID << " stable? " << isstable () << "\n";
  warnx << "# estimated node in ring " << nnodes << "\n";
  warnx << "# getsuccesor requests " << ndogetsuccessor << "\n";
  warnx << "# getpredecessor requests " << ndogetpredecessor << "\n";
  warnx << "# findclosestpred requests " << ndofindclosestpred << "\n";
  warnx << "# notify requests " << ndonotify << "\n";  
  warnx << "# alert requests " << ndoalert << "\n";  
  warnx << "# testrange requests " << ndotestrange << "\n";  
  warnx << "# getfingers requests " << ndogetfingers << "\n";

  warnx << "# getsuccesor calls " << ngetsuccessor << "\n";
  warnx << "# getpredecessor calls " << ngetpredecessor << "\n";
  warnx << "# findsuccessor calls " << nfindsuccessor << "\n";
  warnx << "# hops for findsuccessor " << nhops << "\n";
  {
    char buf[100];
    if (nfindsuccessor)
      sprintf (buf, "   Average # hops: %f\n", ((float)(nhops/nfindsuccessor)));
    warnx << buf;
  }
  warnx << "   # max hops for findsuccessor " << nmaxhops << "\n";
  warnx << "# findpredecessor calls " << nfindpredecessor << "\n";
  warnx << "# findsuccessorrestart calls " << nfindsuccessorrestart << "\n";
  warnx << "# findpredecessorrestart calls " << nfindpredecessorrestart << "\n";
  warnx << "# rangandtest calls " << ntestrange << "\n";
  warnx << "# notify calls " << ndonotify << "\n";  
  warnx << "# alert calls " << ndoalert << "\n";  
  warnx << "# getfingers calls " << ndogetfingers << "\n";
}

void
vnode::print ()
{
  warnx << "======== " << myID << "====\n";
  for (int i = 1; i <= NBIT; i++) {
    if (!finger_table[i].first.alive) continue;
    if ((finger_table[i].first.n != finger_table[i-1].first.n) 
	|| !finger_table[i-1].first.alive) {
      warnx << "finger " << i << ": " << finger_table[i].first.n << "\n";
    }
  }
  for (int i = 1; i <= nsucc; i++) {
    if (!succlist[i].alive) continue;
    warnx << "succ " << i << " : " << succlist[i].n << "\n";
  }
  warnx << "pred : " << predecessor.n << "\n";
  warnx << "=====================================================\n";
}

chordID
vnode::findsuccfinger (chordID &x)
{
  chordID s = x;
  for (int i = 1; i <= NBIT; i++) {
    if (!finger_table[i].first.alive) continue;
    if ((s == x) || between (x, s, finger_table[i].first.n)) {
      s = finger_table[i].first.n;
    }
  }
  for (int i = 1; i <= nsucc; i++) {
    if ((succlist[i].alive) && between (x, s, succlist[i].n)) {
      s = succlist[i].n;
    }
  }
  //  warnx << "findsuccfinger: " << myID << " of " << x << " is " << s << "\n";
  return s;
}

chordID 
vnode::findpredfinger (chordID &x)
{
  chordID p = myID;
  for (int i = NBIT; i >= 1; i--) {
    if ((finger_table[i].first.alive) && 
	between (myID, x, finger_table[i].first.n)) {
      p = finger_table[i].first.n;
      return p;
    }
  }
  // no good entries in my finger table (e.g., fingers are down); check succlist
  for (int i = nsucc; i >= 1; i--) {
    if ((succlist[i].alive) && between (p, x, succlist[i].n)) {
      // warnx << "findpredfinger: take entry from succlist\n";
      p = succlist[i].n;
      break;
    }
  }
  return p;
}

chordID 
vnode::findpredfinger2 (chordID &x)
{
  chordID p = myID;
  for (int i = 1; i <= NBIT; i++) {
    if ((finger_table[i].first.alive) && 
	locations->betterpred2 (myID, p, x, finger_table[i].first.n)) {
      p = finger_table[i].first.n;
    }
  }
  // warnx << "findpredfinger2: " << myID << " of " << x << " is " << p << "\n";
  return p;
}

u_long
vnode::estimate_nnodes () 
{
  u_long n;
  chordID d = diff (myID, succlist[nsucc].n);
  if ((d > 0) && (nsucc > 0)) {
    chordID s = d / nsucc;
    chordID c = bigint (1) << NBIT;
    chordID q = c / s;
    n = q.getui ();
  } else n = 1;
  return n;
}

chordID
vnode::lookup_closestsucc (chordID &x)
{
#ifdef PNODE
  chordID s = findsuccfinger (x);
#else
  chordID s = locations->findsuccloc (x);
#endif
  return s;
}

chordID
vnode::lookup_closestpred (chordID &x)
{
#ifdef PNODE
  chordID p = findpredfinger (x);
#else
  chordID p = locations->findpredloc (x);
#endif
  return p;
}

bool
vnode::isstable (void) 
{
  return stable_fingers && stable_fingers2 && stable_succlist && 
    stable_succlist2;
}

bool
vnode::hasbecomeunstable (void)
{
  return ((!stable_fingers && stable_fingers2) ||
	  (!stable_succlist && stable_succlist2));
}

void
vnode::stabilize (void)
{
  stabilize_continuous ();
  stabilize_backoff (0, 0, stabilize_timer);
}

void
vnode::stabilize_continuous (void)
{
#ifndef STOPSTABILIZE
  stabilize_succ ();
  stabilize_pred ();
  if (hasbecomeunstable () && stabilize_backoff_tmo) {
    //    warnx << "stabilize_continuous: cancel backoff timer and restart\n";
    timecb_remove (stabilize_backoff_tmo);
    stabilize_backoff_tmo = 0;
    stabilize_backoff (0, 0, stabilize_timer);
  }
  u_int32_t t1 = uniform_random (0.5 * stabilize_timer, 1.5 * stabilize_timer);
  u_int32_t sec = t1 / 1000;
  u_int32_t nsec =  (t1 % 1000) * 1000000;
  //  warnx << "stabilize_continuous: sec " << sec << " nsec " << nsec << "\n";
  stabilize_continuous_tmo = delaycb (sec, nsec, wrap (mkref (this), 
					       &vnode::stabilize_continuous));
#endif
}

void
vnode::stabilize_backoff (int f, int s, u_int32_t t)
{
  stabilize_backoff_tmo = 0;
  if (!stable && isstable ()) {
    stable = true;
    warnx << gettime () << " stabilize: " << myID 
	  << " stable! with estimate # nodes " << nnodes << "\n";
    print ();
  } else if (!isstable ()) {
    t = stabilize_timer;
    stable = false;
  }
#ifdef STOPSTABILIZE
  stabilize_succ ();
  stabilize_pred ();
#endif
  f = stabilize_finger (f);
  s = stabilize_succlist (s);
#ifdef STOPSTABILIZE
  if (isstable ()) { 
    t = 2 * t;
    if (t >= 50000) {
      warnx << "stabilize_backoff: " << myID << " stop stabilizing\n";
      return;
    }
  }
  u_int32_t t1 = uniform_random (0.5 * stabilize_timer, 1.5 * stabilize_timer);
#else
  if (isstable () && (t <= stabilize_timer_max * 1000)) {
    t = 2 * t;
  }
  u_int32_t t1 = uniform_random (0.5 * t, 1.5 * t);
#endif
  // warnx << myID << ": delay till " << t1 << " msec\n";
  u_int32_t sec = t1 / 1000;
  u_int32_t nsec =  (t1 % 1000) * 1000000;
  // warnx << "stabilize_backoff: sec " << sec << " nsec " << nsec << "\n";
  stabilize_backoff_tmo = delaycb (sec, nsec, wrap (mkref (this), 
					       &vnode::stabilize_backoff,
					       f, s, t));
}

void
vnode::stabilize_succ ()
{
  while (!finger_table[1].first.alive) {   // notify() may result in failure
    //  warnx << "stabilize: replace succ\n";
    replacefinger (&finger_table[1].first);
    notify (finger_table[1].first.n, myID); 
  }
  get_predecessor (finger_table[1].first.n, 
		   wrap (mkref (this), &vnode::stabilize_getpred_cb));
}

void
vnode::stabilize_getpred_cb (chordID p, net_address r, chordstat status)
{
  // receive predecessor from my successor; in stable case it is me
  if (status) {
    warnx << "stabilize_getpred_cb: " << myID << " " 
	  << finger_table[1].first.n << " failure " << status << "\n";
    stable_fingers = false;
  } else {
    if (!finger_table[1].first.alive || (finger_table[1].first.n == myID) ||
	between (myID, finger_table[1].first.n, p)) {
      // warnx << "stabilize_pred_cb: " << myID << " new successor is "
      //   << p << "\n";
      locations->changenode (&finger_table[1].first, p, r);
      updatefingers (p, r);
      stable_fingers = false;
    }
    notify (finger_table[1].first.n, myID);
  }
}

int
vnode::stabilize_finger (int f)
{
  int i = f % (NBIT+1);

  if (i == 0) {
    if (stable_fingers) stable_fingers2 = true;
    else stable_fingers2 = false;
    stable_fingers = true;
  }

  if (i <= 1) i = 2;		// skip myself and immediate successor

  if (!finger_table[i].first.alive) {
    //  warnx << "stabilize: replace finger " << i << "\n" ;
    replacefinger (&finger_table[i].first);
    stable_fingers = false;
  }
  if (i > 1) {
    for (; i <= NBIT; i++) {
      if (!finger_table[i-1].first.alive) break;
      if (between (finger_table[i-1].start, finger_table[i-1].first.n,
		   finger_table[i].start)) {
	chordID s = finger_table[i-1].first.n;
	if (finger_table[i].first.n != s) {
	  locations->changenode (&finger_table[i].first, s, 
				 locations->getaddress(s));
	  updatefingers (s, locations->getaddress(s));
	  stable_fingers = false;
	}
      } else break;
    }
    if (i <= NBIT) {
      // warnx << "stabilize: " << myID << " findsucc of finger " << i << "\n";
      find_successor (myID, finger_table[i].start,
			  wrap (mkref (this), 
				&vnode::stabilize_findsucc_cb, i));
      i++;
    }
  }
  return i;
}

void
vnode::stabilize_findsucc_cb (int i, chordID s, route search_path, 
			    chordstat status)
{
  if (status) {
    warnx << "stabilize_findsucc_cb: " << myID << " " 
	  << finger_table[i].first.n << " failure " << status << "\n";
    stable_fingers = false;
  } else {
    if (!finger_table[i].first.alive || (finger_table[i].first.n != s)) {
      // warnx << "stabilize_findsucc_cb: " << myID << " " 
      //   << "new successor of " << finger_table[i].start 
      //    << " is " << s << "\n";
      locations->changenode (&finger_table[i].first, s, 
			     locations->getaddress(s));
      updatefingers (s, locations->getaddress(s));
      stable_fingers = false;
    }
  }
}

void
vnode::stabilize_pred ()
{
  if (predecessor.alive) {
    get_successor (predecessor.n,
		   wrap (mkref (this), &vnode::stabilize_getsucc_cb));
  } else 
    stable_fingers = false;
}

void
vnode::stabilize_getsucc_cb (chordID s, net_address r, chordstat status)
{
  // receive successor from my predecessor; in stable case it is me
  if (status) {
    warnx << "stabilize_getpred_cb: " << myID << " " << predecessor.n 
	  << " failure " << status << "\n";
    stable_fingers = false;
  } else {
    if (s != myID) {
      stable_fingers = false;
      // warnx << "stabilize_succ_cb: " << myID << " my pred " 
      //   << predecessor.n << " has " << s << " as succ\n";
    }
  }
}

int
vnode::stabilize_succlist (int s)
{
  int j = s % (nsucc+1);

  if (j == 0) {
    if (stable_succlist) stable_succlist2 = true;
    else stable_succlist2 = false;
    stable_succlist = true;
  }
  if (!succlist[j].alive) {
    //  warnx << "stabilize: replace succ " << j << "\n";
    stable_succlist = false;
    replacefinger (&succlist[j]);
  }
  get_successor (succlist[j].n,
		 wrap (mkref (this), &vnode::stabilize_getsucclist_cb, j));
  return j+1;
}


void
vnode::stabilize_getsucclist_cb (int i, chordID s, net_address r, 
			       chordstat status)
{
  if (status) {
    warnx << "stabilize_getsucclist_cb: " << myID << " " << i << " : " 
	  << succlist[i].n << " failure " << status << "\n";
    stable_succlist = false;
  } else {
    //    warnx << "stabilize_getsucclist_cb: " << myID << " " << i 
    //	  << " : successor of " 
    //	  << succlist[i].n << " is " << s << "\n";
    if (s == myID) {  // did we go full circle?
      if (nsucc > i) {  // remove old entries?
	stable_succlist = false;
	for (int j = nsucc+1; j <= NSUCC; j++) {
	  if (succlist[j].alive) {
	    locations->deleteloc (succlist[j].n);
	    succlist[j].alive = false;
	  }
	}
      }
      nsucc = i;
    } else if (i < NSUCC) {
      if (succlist[i+1].n != s) {
	stable_succlist = false;
	locations->changenode (&succlist[i+1], s, r);
      }
      if ((i+1) > nsucc) {
	stable_succlist = false;
	nsucc = i+1;
      }
    }
    u_long n = estimate_nnodes ();
    locations->replace_estimate (nnodes, n);
    nnodes = n;
  }
}

void
vnode::join (cbjoin_t cb)
{
  chordID n;

  if (!locations->lookup_anyloc(myID, &n))
    fatal ("No nodes left to join\n");
  // warn << "find succ from join " << n << " " << myID << "\n";
  find_successor (n, myID, wrap (mkref (this), &vnode::join_getsucc_cb, cb));
}

void 
vnode::join_getsucc_cb (cbjoin_t cb, chordID s, route r, chordstat status)
{
  if (status) {
    warnx << "join_getsucc_cb: " << myID << " " << status << "\n";
    join (cb);  // try again
  } else {
    //    warnx << "join_getsucc_cb: " << myID << " " << s << "\n";
    locations->changenode (&finger_table[1].first, s, locations->getaddress(s));
    updatefingers (s, locations->getaddress(s));
    stabilize ();
    (*cb) (this);
  }
}


void
vnode::doget_successor (svccb *sbp)
{
  ndogetsuccessor++;
  if (finger_table[1].first.alive) {
    chordID s = finger_table[1].first.n;
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

  if (finger_table[1].first.alive && 
      betweenrightincl(myID, finger_table[1].first.n, x) ) {
    res = New chord_testandfindres (CHORD_INRANGE);
    warnt("CHORD: testandfind_inrangereply");
    //    warnx << "dotestrange_findclosestpred: " << myID << " succ of " << x 
    //  << " is " << finger_table[1].first.n << "\n";
    res->inres->x = finger_table[1].first.n;
    res->inres->r = locations->getaddress (finger_table[1].first.n);
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
  warnx << "dofindclosestpred: " << myID << " closest pred of " << fa->x 
	<< " is " << p << "\n";
  warnt("CHORD: dofindclosestpred_reply");
  sbp->reply (&res);
}

void
vnode::donotify (svccb *sbp, chord_nodearg *na)
{
  ndonotify++;
  if ((!predecessor.alive) || (predecessor.n == myID) || 
      between (predecessor.n, myID, na->n.x)) {
    // warnx << "donotify: updated predecessor: new pred is " << na->n.x 
    //       << "\n";
    locations->changenode (&predecessor, na->n.x, na->n.r);
    if (predecessor.n != myID) {
      updatefingers (predecessor.n, na->n.r);
      get_fingers (predecessor.n); // XXX perhaps do this only once after join
    }
  }
  sbp->replyref (chordstat (CHORD_OK));
}

void
vnode::doalert (svccb *sbp, chord_nodearg *na)
{
  ndoalert++;
  warnt("CHORD: doalert");
  // XXX perhaps less aggressive and check status of x first
  chordnode->deletefingers (na->n.x);
  sbp->replyref (chordstat (CHORD_OK));
}

void
vnode::dogetfingers (svccb *sbp)
{
  chord_getfingersres res(CHORD_OK);
  ndogetfingers++;
  int n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!finger_table[i].first.alive) continue;
    if (finger_table[i].first.n != finger_table[i-1].first.n) {
      n++;
    }
  }
  res.resok->fingers.setsize (n);
  res.resok->fingers[0].x = finger_table[0].first.n;
  res.resok->fingers[0].r = locations->getaddress (finger_table[0].first.n);
  n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!finger_table[i].first.alive) continue;
    if (finger_table[i].first.n != finger_table[i-1].first.n) {
      res.resok->fingers[n].x = finger_table[i].first.n;
      res.resok->fingers[n].r = locations->getaddress (finger_table[i].first.n);
      n++;
    }
  }
  warnt("CHORD: dogetfingers_reply");
  sbp->reply (&res);
}

void
vnode::dofindsucc (chordID &n, cbroute_t cb)
{
  // warn << "dofindsucc " << myID << " " << n << "\n";
  find_successor (myID, n, wrap (mkref (this), &vnode::dofindsucc_cb, cb, n));
}

void
vnode::dofindsucc_cb (cbroute_t cb, chordID n, chordID x,
                    route search_path, chordstat status) 
{
  if (status) {
    warnx << "dofindsucc_cb for " << n << " returned " <<  status << "\n";
    if (status == CHORD_RPCFAILURE) {
      warnx << "dofindsucc_cb: try to recover\n";
      chordID last = search_path.pop_back ();
      chordID lastOK = search_path.back ();
      warnx << "dofindsucc_cb: last node " << last << " contacted failed\n";
      alert (lastOK, last);
      find_successor_restart (lastOK, x, search_path,
                              wrap (mkref (this), &vnode::dofindsucc_cb, cb, 
				    n));
    } else {
      cb (x, search_path, CHORD_ERRNOENT);
    }
  } else {
    // warnx << "dofindsucc_cb: " << myID << " done\n";
    // for (unsigned i = 0; i < search_path.size (); i++) {
    // warnx << search_path[i] << "\n";
    // }
    cb (x, search_path, CHORD_OK);
  }
}

