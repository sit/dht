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
  if (finger_table[1].first.alive) return finger_table[1].first.n;
  for (int i = 1; i <= nsucc; i++) {
    if (succlist[i].alive) return succlist[i].n;
  }
  return myID;
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
vnode::checkfingers ()
{
  int j;
  int i;
  for (i = 1; i <= NBIT; i++) {
    if (finger_table[i].first.n == myID) continue;
    else break;
  }
  if (i > NBIT) return;

  for (i = 1; i <= NBIT; i++) {
    if (!finger_table[i].first.alive) continue;
    j = i+1;
    while ((j <= NBIT) && !finger_table[j].first.alive) j++;
    if (j > NBIT) {
      if (!betweenrightincl (finger_table[i].start, myID, 
                             finger_table[i].first.n)) {
        warnx << "table " << i << " bad\n";
        warnx << "start " << finger_table[i].start << "\n";
        warnx << "first " << finger_table[i].first.n << "\n";
        print ();
        assert (0);
      }
    } else {
      if (finger_table[j].first.n == myID) {
        return;
      }
      if (!betweenrightincl (finger_table[i].start, finger_table[j].first.n,
                 finger_table[i].first.n)) {
        warnx << "table " << i << " bad\n";
        print ();
        assert (0);
      }
    }
  }
}

void 
vnode::updatefingers (chordID &x, net_address &r)
{
  checkfingers();
  for (int i = 1; i <= NBIT; i++) {
    if (betweenleftincl (finger_table[i].start, finger_table[i].first.n, x)) {
      locations->changenode (&finger_table[i].first, x, r);
    }
  }
  checkfingers();
}

void
vnode::replacefinger (chordID &s, node *n)
{  
  checkfingers ();
#ifdef PNODE
  n->n = closestsuccfinger (s);
#else
  n->n = locations->closestsuccloc (s);
#endif
  n->alive = true;
  locations->increfcnt (n->n);
  checkfingers ();
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
    if (nfindsuccessor)
      sprintf (buf, "   Average # hops: %f\n", ((float) nhops)/nfindsuccessor);
    warnx << buf;
  }
  warnx << "   # max hops for findsuccessor " << nmaxhops << "\n";
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
vnode::stop ()
{
  if (stabilize_continuous_tmo) {
    warnx << "stop " << myID << " switch off cont stabilize timer\n";
    timecb_remove (stabilize_continuous_tmo);
    stabilize_continuous_tmo = NULL;
  }
  if (stabilize_backoff_tmo) {
    warnx << "stop " << myID << " switch off backoff stabilize timer\n";
    timecb_remove (stabilize_backoff_tmo);
    stabilize_backoff_tmo = NULL;
  }
}

void
vnode::print ()
{
  warnx << "======== " << myID << "====\n";
  for (int i = 1; i <= NBIT; i++) {
    if (!finger_table[i].first.alive) continue;
    warnx << "finger: " << i << " : " << finger_table[i].start << " : succ " 
	  << finger_table[i].first.n << "\n";
    //    if ((finger_table[i].first.n != finger_table[i-1].first.n) 
    //|| !finger_table[i-1].first.alive) {
    //warnx << "first " << i << ": " << finger_table[i].first.n << "\n";
    //}
  }
  for (int i = 1; i <= nsucc; i++) {
    if (!succlist[i].alive) continue;
    warnx << "succ " << i << " : " << succlist[i].n << "\n";
  }
  warnx << "pred : " << predecessor.n << "\n";
  warnx << "=====================================================\n";
}

chordID
vnode::closestsuccfinger (chordID &x)
{
  chordID s = x;
  for (int i = 0; i <= NBIT; i++) {
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
  //  warnx << "cloesestsuccfinger: " << myID << " of " << x << " is " << s << "\n";
  return s;
}

chordID 
vnode::closestpredfinger (chordID &x)
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
      p = succlist[i].n;
      break;
    }
  }
  return p;
}

chordID 
vnode::closestpredfinger_ss (chordID &x)
{
  chordID p = myID;
  char better = 0;
  char best = 0;
  for (int i = 1; i <= NBIT; i++) {
    if (finger_table[i].first.alive && (p != finger_table[i].first.n)) {
      if (server_selection_mode == 1)
	better = locations->betterpred2 (myID, p, x, finger_table[i].first.n);
      else if (server_selection_mode == 2)
	better = locations->betterpred3 (myID, p, x, finger_table[i].first.n);
      else if (server_selection_mode == 3)
	better = locations->betterpred_distest (myID, p, x, 
						finger_table[i].first.n);
      else
	better = locations->betterpred_greedy (myID, p, x, 
					       finger_table[i].first.n);
    
      if ((finger_table[i].first.alive) && better) {
	p = finger_table[i].first.n;
	best = better;
      }
    }

  }


#ifdef VERYVERBOSE
  if (best) {
    warn << "chose " << p << " because ";
    switch (best) {
    case 1:
      warn << "it was my first pred.\n";
      break;
    case 2:
      warn << "I had no latency info\n";
      break;
    case 3:
      warn << "the estimate was lower\n";
      break;
    default:
      warn << "there was a bug in my code\n";
    }
  }

  location *choice = locations->getlocation (p);

  char buf[1024];
  if (choice->nrpc)
    sprintf (buf, "%f", (float)choice->rpcdelay/choice->nrpc);
  else
    sprintf (buf, " (no latency info) ");

  if (choice->nrpc && (float)choice->rpcdelay/choice->nrpc > 50000)
    warn << "LONG HOP " << buf << "\n";
  else
    warn << "SHORT HOP " << buf << "\n";
#endif

  if (p != myID) return p;

  for (int i = nsucc; i >= 1; i--) {
    bool better;
    if (server_selection_mode == 1)
      better = locations->betterpred2 (myID, p, x, finger_table[i].first.n);
    else if (server_selection_mode == 2)
      better = locations->betterpred3 (myID, p, x, finger_table[i].first.n);
    else 
      better = locations->betterpred_greedy (myID, p, x, 
					     finger_table[i].first.n);
    if ((succlist[i].alive) && better) {
      p = succlist[i].n;
      break;
    }
  }
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
  chordID s = closestsuccfinger (x);
#else
  chordID s = locations->closestsuccloc (x);
#endif
  return s;
}

chordID
vnode::lookup_closestpred (chordID &x)
{
#ifdef PNODE
  if (server_selection_mode) 
    return closestpredfinger_ss (x);
  else
    return closestpredfinger (x);

#else
  return locations->closestpredloc (x);
#endif

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
  stabilize_continuous (stabilize_timer);
  stabilize_backoff (0, 0, stabilize_timer);
}

void
vnode::stabilize_continuous (u_int32_t t)
{
  stabilize_continuous_tmo = NULL;
  if (nout_continuous > 0) {
    // stabilizing too fast
    t = 2 * t;
  } else {
    stabilize_succ ();
    stabilize_pred ();
  }
  if (hasbecomeunstable () && stabilize_backoff_tmo) {
    timecb_remove (stabilize_backoff_tmo);
    stabilize_backoff_tmo = 0;
    stabilize_backoff (0, 0, stabilize_timer);
  }
  u_int32_t t1 = uniform_random (0.5 * t, 1.5 * t);
  u_int32_t sec = t1 / 1000;
  u_int32_t nsec =  (t1 % 1000) * 1000000;
  continuous_timer = t;
  stabilize_continuous_tmo = delaycb (sec, nsec, 
				      wrap (mkref (this), 
					    &vnode::stabilize_continuous, t));
}

void
vnode::stabilize_succ ()
{
  while (!finger_table[1].first.alive) {   // notify() may result in failure
    replacefinger (finger_table[1].start, &finger_table[1].first);
    notify (finger_table[1].first.n, myID); 
  }
  nout_continuous++;
  get_predecessor (finger_table[1].first.n, 
		   wrap (mkref (this), &vnode::stabilize_getpred_cb));
}

void
vnode::stabilize_getpred_cb_ok (chordID p, bool ok, chordstat status)
{
  nout_continuous--;
  if ((status == CHORD_OK) && ok) {
    if (betweenleftincl (finger_table[1].start, finger_table[1].first.n, p)) {
      // warnx << "stabilize_pred_cb_ok: " << myID << " new successor is "
      //   << p << "\n";
      net_address r = locations->getaddress (p);
      locations->changenode (&finger_table[1].first, p, r);
      updatefingers (p, r);
      stable_fingers = false;
      notify (finger_table[1].first.n, myID);
    }
  }
}

void
vnode::stabilize_getpred_cb (chordID p, net_address r, chordstat status)
{
  // receive predecessor from my successor; in stable case it is me
  if (status) {
    warnx << "stabilize_getpred_cb: " << myID << " " 
	  << finger_table[1].first.n << " failure " << status << "\n";
    stable_fingers = false;
    nout_continuous--;
  } else {
    //    warnx << "stabilize_pred_cb: s " << finger_table[1].start << " first "
    //  << finger_table[1].first.n << " p " << p << "\n";
    if (betweenleftincl (finger_table[1].start, finger_table[1].first.n, p)) {
      //      warnx << "stabilize_pred_cb: " << myID << " check " << p 
      //    << "'s identity\n";
      locations->cacheloc (p, r);
      challenge (p, wrap (mkref (this), &vnode::stabilize_getpred_cb_ok));
    } else {
      nout_continuous--;
      notify (finger_table[1].first.n, myID);
    }
  }
}

void
vnode::stabilize_pred ()
{
  if (predecessor.alive) {
    nout_continuous++;
    get_successor (predecessor.n,
		   wrap (mkref (this), &vnode::stabilize_getsucc_cb));
  } else 
    stable_fingers = false;
}

void
vnode::stabilize_getsucc_cb (chordID s, net_address r, chordstat status)
{
  // receive successor from my predecessor; in stable case it is me
  nout_continuous--;
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

/*
 * RSC: I think that we should be using some sort of AI/MD so that
 * if things start changing we can adapt.  Right now once we get slow
 * we stay slow forever.  At the least, we need some sort of increase
 * (right now we're just MD).  However, if I turn on aimd, then the
 * Chord finger tables start dying, presumably because the increased
 * update frequency tickles a bug elsewhere.  That's a project for
 * another day.
 */
#define aimd 0
void
vnode::stabilize_backoff (int f, int s, u_int32_t t)
{
  stabilize_backoff_tmo = 0;
  if (!stable && isstable ()) {
    stable = true;
    warnx << gettime () << " stabilize: " << myID 
	  << " stable! with estimate # nodes " << nnodes << "\n";
  } else if (!isstable ()) {
    stable = false;
  }
  if (nout_backoff > 0) {
    if(aimd)
      t = (int)(1.2 * t);
    else
     t *= 2;
    // warnx << "stabilize_backoff: " << myID << " " << nout_backoff 
    //  << " slow down " << t << "\n";
  } else {
    f = stabilize_finger (f);
    s = stabilize_succlist (s);
    if (isstable () && (t <= stabilize_timer_max * 1000))
      if(aimd)
        t = (int)(1.2 * t);
      else
        t *= 2;
    else if (aimd && t > 100)
      t -= 100;
  }
  u_int32_t t1 = uniform_random (0.5 * t, 1.5 * t);
  u_int32_t sec = t1 / 1000;
  u_int32_t nsec =  (t1 % 1000) * 1000000;
  backoff_timer = t;
  stabilize_backoff_tmo = delaycb (sec, nsec, wrap (mkref (this), 
						    &vnode::stabilize_backoff,
						    f, s, t));
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
    replacefinger (finger_table[i].start, &finger_table[i].first);
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
      nout_backoff++;
      find_successor (finger_table[i].start, wrap (mkref (this), 
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
  nout_backoff--;
  if (status) {
    warnx << "stabilize_findsucc_cb: " << myID << " " 
	  << finger_table[i].first.n << " failure " << status << "\n";
    stable_fingers = false;
  } else {
    if (betweenleftincl (finger_table[i].start, finger_table[i].first.n, s)) {
      challenge (s, wrap (mkref (this), &vnode::stabilize_findsucc_ok, i));
    }
  }
}

void
vnode::stabilize_findsucc_ok (int i, chordID s, bool ok, chordstat status)
{
  if ((status == CHORD_OK) && ok) {
    if (betweenleftincl (finger_table[i].start, finger_table[i].first.n, s)) {
      // warnx << "stabilize_findsucc_ok: " << myID << " " 
      //   << "new successor of " << finger_table[i].start 
      //    << " is " << s << "\n";
      locations->changenode (&finger_table[i].first, s, 
			     locations->getaddress(s));
      updatefingers (s, locations->getaddress(s));
      stable_fingers = false;
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
    replacefinger (succlist[j].n, &succlist[j]);
  }
  nout_backoff++;
  get_successor (succlist[j].n,
		 wrap (mkref (this), &vnode::stabilize_getsucclist_cb, j));
  return j+1;
}


void
vnode::stabilize_getsucclist_cb (int i, chordID s, net_address r, 
			       chordstat status)
{
  nout_backoff--;
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
	locations->cacheloc (s, r);
	challenge (s, wrap (mkref (this), &vnode::stabilize_getsucclist_ok, 
			    i+1));
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
vnode::stabilize_getsucclist_ok (int i, chordID s, bool ok, chordstat status)
{
  if ((status == CHORD_OK) && ok) {
    if (succlist[i].n != s) {
	stable_succlist = false;
	locations->changenode (&succlist[i], s, locations->getaddress (s));
    }
  }
}

void
vnode::join (cbjoin_t cb)
{
  chordID n;

  if (!locations->lookup_anyloc (myID, &n)) {
    (*cb)(NULL, CHORD_ERRNOENT);
  } else {
    net_address r = locations->getaddress (n);
    updatefingers (n, r);
    // warn << "find succ for me " << myID << "\n";
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
    //    locations->cacheloc (na->n.x, na->n.r);
    challenge (s, wrap (mkref (this), &vnode::join_getsucc_ok, cb));
  }
}

void
vnode::join_getsucc_ok (cbjoin_t cb, chordID s, bool ok, chordstat status)
{
  if ((status == CHORD_OK) && ok) {
    //  warnx << "join_getsucc_ok: " << myID << " " << s << "\n";
    if (betweenleftincl (finger_table[1].start, finger_table[1].first.n, s)) {
      // the successor that we found is better than the one from location table
      locations->changenode (&finger_table[1].first, s, 
			     locations->getaddress(s));
      updatefingers (s, locations->getaddress(s));
    }
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
      updatefingers (predecessor.n, r);
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
vnode::dochallenge (svccb *sbp, chord_challengearg *ca)
{
  chord_challengeres res(CHORD_OK);
  ndochallenge++;
  res.resok->index = myindex;
  res.resok->challenge = ca->challenge;
  sbp->reply (&res);
}

