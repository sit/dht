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

vnode::vnode (ptr<locationtable> _locations, ptr<chord> _chordnode,
	      chordID _myID) :
  locations (_locations),
  chordnode (_chordnode),
  myID (_myID)
{
  warnx << "myID is " << myID << "\n";
  finger_table[0].start = finger_table[0].first.n = myID;
  finger_table[0].first.alive = true;
  locations->increfcnt (myID);
  succlist[0].n = myID;
  succlist[0].alive = true;
  nsucc = 0;
  locations->increfcnt (myID);
  for (int i = 1; i <= NBIT; i++) {
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
vnode::deletefingers (chordID &x)
{
  assert (x != myID);
  for (int i = 1; i <= NBIT; i++) {
    if (finger_table[i].first.alive && (x == finger_table[i].first.n)) {
      locations->deleteloc (finger_table[i].first.n);
      finger_table[i].first.alive = false;
    }
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
  warnx << "VIRTUAL NODE STATS " << myID << "\n";
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
  fprintf(stderr, "   Average # hops: %f\n", ((float)(nhops/nfindsuccessor)));
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
    warnx << "succ " << i << " : " << succlist[i].n << "\n";
  }
  warnx << "pred : " << predecessor.n << "\n";
  warnx << "=====================================================\n";
}

chordID 
vnode::findpredfinger (chordID &x)
{
  chordID p = myID;
  for (int i = NBIT; i >= 0; i--) {
    if ((finger_table[i].first.alive) && 
	between (myID, x, finger_table[i].first.n)) {
      p = finger_table[i].first.n;
      break;
    }
  }
  warnx << "findpredfinger: " << myID << " of " << x << " is " << p << "\n";
  return p;
}

void
vnode::stabilize (int c)
{
  int i = c % (NBIT+1);
  int j = c % (nsucc+1);

  warnt("CHORD: stabilize");
  warnx << "stabilize: " << myID << " " << i << "\n";
  print ();

  locations->checkrefcnt (1);

  while (!finger_table[1].first.alive) {   // notify() may result in failure
    warnx << "stabilize: replace succ\n";
    locations->replacenode (&finger_table[1].first);
    notify (finger_table[1].first.n, myID); 
  }
  get_predecessor (finger_table[1].first.n, 
		   wrap (mkref (this), &vnode::stabilize_getpred_cb));
  if (!finger_table[i].first.alive) {
    warnx << "stabilize: replace finger " << i << "\n" ;
    locations->replacenode (&finger_table[i].first);
  }
  if (i > 1) {
    find_successor (myID, finger_table[i].start,
			  wrap (mkref (this), 
				&vnode::stabilize_findsucc_cb, i));
  }
  if (predecessor.alive) {
    get_successor (predecessor.n,
		   wrap (mkref (this), &vnode::stabilize_getsucc_cb));
  }
  if (!succlist[j].alive) {
    warnx << "stabilize: replace succ " << j << "\n";
    locations->replacenode (&succlist[j]);
  }
  get_successor (succlist[j].n,
		 wrap (mkref (this), &vnode::stabilize_getsucclist_cb, j));
  int time = uniform_random (0.5 * stabilize_timer, 1.5 * stabilize_timer);
  stabilize_tmo = delaycb (0, time * 1000000, 
			   wrap (mkref (this), &vnode::stabilize, i+1));
  locations->checkrefcnt (2);
}

void
vnode::stabilize_getpred_cb (chordID p, net_address r, chordstat status)
{
  // receive predecessor from my successor; in stable case it is me
  if (status) {
    warnx << "stabilize_getpred_cb: " << myID << " " 
	  << finger_table[1].first.n << " failure " << status << "\n";
  } else {
    if (!finger_table[1].first.alive || (finger_table[1].first.n == myID) ||
	between (myID, finger_table[1].first.n, p)) {
      warnx << "stabilize_pred_cb: new successor is " << p << "\n";
      locations->changenode (&finger_table[1].first, p, r);
      updatefingers (p, r);
      print ();
    }
    notify (finger_table[1].first.n, myID);
  }
}

void
vnode::stabilize_findsucc_cb (int i, chordID s, route search_path, 
			    chordstat status)
{
  if (status) {
    warnx << "stabilize_findsucc_cb: " << myID << " " 
	  << finger_table[i].first.n << " failure " << status << "\n";
  } else {
    if (!finger_table[i].first.alive || (finger_table[i].first.n != s)) {
      warnx << "stabilize_findsucc_cb: " << myID << " " 
	    << "new successor of " << finger_table[i].start 
	    << " is " << s << "\n";
      locations->changenode (&finger_table[i].first, s, 
			     locations->getaddress(s));
      updatefingers (s, locations->getaddress(s));
      print ();
    }
  }
}

void
vnode::stabilize_getsucc_cb (chordID s, net_address r, chordstat status)
{
  // receive successor from my predecessor; in stable case it is me
  if (status) {
    warnx << "stabilize_getpred_cb: " << myID << " " << predecessor.n 
	  << " failure " << status << "\n";
  } else {
    if (s != myID) {
      warnx << "stabilize_succ_cb: " << myID << " WEIRD my pred " 
	    << predecessor.n << " has " << s << " as succ\n";
    }
  }
}

void
vnode::stabilize_getsucclist_cb (int i, chordID s, net_address r, 
			       chordstat status)
{
  if (status) {
    warnx << "stabilize_getsucclist_cb: " << myID << " " << i << " : " 
	  << succlist[i].n << " failure " << status << "\n";
  } else {
    warnx << "stabilize_getsucclist_cb: " << myID << " " << i 
	  << " : successor of " 
	  << succlist[i].n << " is " << s << "\n";
    locations->checkrefcnt (3);
    if (s == myID) {  // did we go full circle?
      if (nsucc > i) {  // remove old entries?
	for (int j = nsucc+1; j <= NBIT; j++) {
	  if (succlist[j].alive) {
	    locations->deleteloc (succlist[j].n);
	    succlist[j].alive = false;
	  }
	}
      }
      nsucc = i;
      locations->checkrefcnt (4);
    } else if (i < NBIT) {
      locations->changenode (&succlist[i+1], s, r);
      if ((i+1) > nsucc) nsucc = i+1;
      locations->checkrefcnt (5);
    }
  }
}

void
vnode::join ()
{
  chordID n;

  if (!locations->lookup_anyloc(myID, &n))
    fatal ("No nodes left to join\n");
  find_successor (n, myID, wrap (mkref (this), &vnode::join_getsucc_cb));
}

void 
vnode::join_getsucc_cb (chordID s, route r, chordstat status)
{
  if (status) {
    warnx << "join_getsucc_cb: " << myID << " " << status << "\n";
    join ();  // try again
  } else {
    warnx << "join_getsucc_cb: " << myID << " " << s << "\n";
    locations->changenode (&finger_table[1].first, s, locations->getaddress(s));
    updatefingers (s, locations->getaddress(s));
    print ();
    stabilize (0);
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
vnode::dotestandfind (svccb *sbp, chord_testandfindarg *fa) 
{
  ndotestrange++;
  chordID x = fa->x;
  chord_testandfindres *res;

  if (finger_table[1].first.alive && 
      betweenrightincl(myID, finger_table[1].first.n, x) ) {
    res = New chord_testandfindres (CHORD_INRANGE);
    warnt("CHORD: testandfind_inrangereply");
    warnx << "dotestandfind: " << myID << " succ of " << x << " is " 
	  << finger_table[1].first.n << "\n";
    res->inres->x = finger_table[1].first.n;
    res->inres->r = locations->getaddress (finger_table[1].first.n);
    sbp->reply(res);
    delete res;
  } else {
    res = New chord_testandfindres (CHORD_NOTINRANGE);
    chordID p = locations->findpredloc (fa->x);
    res->noderes->node = p;
    res->noderes->r = locations->getaddress (p);
    warnx << "dotestandfind: " << myID << " closest pred of " << fa->x 
	  << " is " << p << "\n";
    warnt("CHORD: testandfind_notinrangereply");
    sbp->reply(res);
    delete res;
  }
  
}

void
vnode::dofindclosestpred (svccb *sbp, chord_findarg *fa)
{
  chord_noderes res(CHORD_OK);
  chordID p = locations->findpredloc (fa->x);
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
    warnx << "donotify: updated predecessor: new pred is " << na->n.x << "\n";
    locations->changenode (&predecessor, na->n.x, na->n.r);
    if (predecessor.n != myID) {
      updatefingers (predecessor.n, na->n.r);
      print ();
      get_fingers (predecessor.n); // XXX perhaps do this only once after join
    }
  }
  sbp->replyref (chordstat (CHORD_OK));
}

void
vnode::doalert (svccb *sbp, chord_nodearg *na)
{
  ndoalert++;
  assert (0);
  warnt("CHORD: doalert");
  // perhaps less aggressive and check status of x first
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
  // warn << "calling f_s " << predecessor.n << " " << n << "\n";
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
    cb (x, search_path, CHORD_OK);
  }
}

