#include "chord.h"

//#define FINGER_TABLE_CHECK



finger_table::finger_table (ptr<vnode> v,
			    ptr<locationtable> locs,
			    chordID ID)
  : myvnode (v), locations (locs), myID (ID) 
{
  fingers[0].start = fingers[0].first.n = myID;
  fingers[0].first.alive = true;
  locations->increfcnt (myID);

  for (int i = 1; i <= NBIT; i++) {
    locations->increfcnt (myID);
    fingers[i].start = successorID(myID, i-1);
    fingers[i].first.n = myID;
    fingers[i].first.alive = true;
  }

  f = 0;
  stable_fingers = false;
  stable_fingers2 = false;
  nout_continuous = 0;
  nout_backoff = 0;

  nslowfinger = 0;
  nfastfinger = 0;
}

chordID
finger_table::operator[] (int i)
{
  return fingers[i].first.n;
}

bool
finger_table::better_ith_finger (int i, chordID s)
{
  // ASSUMES: s is alive

  if (!alive(i))
    return true;
  else
    return betweenleftincl (fingers[i].start, fingers[i].first.n, s);
}

bool
finger_table::succ_alive () {
  return fingers[1].first.alive;
}

chordID 
finger_table::succ ()
{
  return fingers[1].first.n;
}

void
finger_table::updatefinger (chordID &x)
{
  net_address r = locations->getaddress (x);
  updatefinger (x, r);
}

void 
finger_table::updatefinger (chordID &x, net_address &r)
{
  check ();
  for (int i = 1; i <= NBIT; i++) {
    if (better_ith_finger (i, x)) {
      locations->changenode (&fingers[i].first, x, r);
      stable_fingers = false;
    }
  }
  check ();
}

void
finger_table::replacefinger (int i)
{  
  check ();
#ifdef PNODE
  fingers[i].first.n = closestsuccfinger (fingers[i].start);
#else
  fingers[i].first.n = locations->closestsuccloc (fingers[i].start);
#endif
  fingers[i].first.alive = true;
  locations->increfcnt (fingers[i].first.n);
  check ();
}

void 
finger_table::deletefinger (chordID &x)
{
  // XXX should deleted fingers be replaced immediatedly???
  //     Seems yes if is a large pool of locations recorded
  //     just for cases like these...

  if (x == myID) return;
  check ();
  for (int i = 1; i <= NBIT; i++) {
    if (fingers[i].first.alive && (x == fingers[i].first.n)) {
      locations->deleteloc (fingers[i].first.n);
      fingers[i].first.alive = false;
    }
  }
  check ();
}


chordID
finger_table::closestsuccfinger (chordID &x)
{
  chordID s = x;
  for (int i = 0; i <= NBIT; i++) {
    if (!fingers[i].first.alive) continue;
    if ((s == x) || between (x, s, fingers[i].first.n)) {
      s = fingers[i].first.n;
    }
  }

  return s;
}

chordID 
finger_table::closestpredfinger (chordID &x)
{
  chordID p = myID;
  for (int i = NBIT; i >= 1; i--) {
    if ((fingers[i].first.alive) && 
	between (myID, x, fingers[i].first.n)) {
      p = fingers[i].first.n;
      return p;
    }
  }

  return p;
}

int
finger_table::countrefs (chordID &x)
{
  int n = 0;
  for (int i = 0; i <= NBIT; i++) 
    if (fingers[i].first.alive && (x == fingers[i].first.n))
      n++;

  return n;
}
void
finger_table::check ()
{
#ifndef FINGER_TABLE_CHECK 
  // This takes too much CPU.  [josh]
  return;
#endif

  int j;
  int i;
  // Starting from successor, find first finger who is not me.
  // XXX why? if successor is me, there better not be anyone else around.
  for (i = 1; i <= NBIT; i++) {
    if (fingers[i].first.n == myID) continue;
    else break;
  }
  if (i > NBIT) return;

  for (i = 1; i <= NBIT; i++) {
    if (!fingers[i].first.alive) continue;
    j = i+1;
    while ((j <= NBIT) && !fingers[j].first.alive) j++;
    if (j > NBIT) {
      if (!betweenrightincl (fingers[i].start, myID, 
                             fingers[i].first.n)) {
	warnx << myID << ": finger_table " << i << " bad\n";
	warnx << myID << ": start " << fingers[i].start << "\n";
	warnx << myID << ": first " << fingers[i].first.n << "\n";

        print ();
        assert (0);
      }
    } else {
      if (fingers[j].first.n == myID) {
        return;
      }
      if (!betweenrightincl (fingers[i].start, fingers[j].first.n,
                 fingers[i].first.n)) {
	warnx << myID << ": finger_table " << i << ", " << j << " bad\n";
        print ();
        assert (0);
      }
    }
  }
}

void
finger_table::print ()
{
  for (int i = 1; i <= NBIT; i++) {
    if (!fingers[i].first.alive) continue;
      warnx << myID << ": finger: " << i << " : " << fingers[i].start
	    << " : succ " << fingers[i].first.n << "\n";
  }
}

void
finger_table::fill_getfingersres (chord_getfingersres *res)
{

  int n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!fingers[i].first.alive) continue;
    if (fingers[i].first.n != fingers[i-1].first.n) {
      n++;
    }
  }
  res->resok->fingers.setsize (n);
  res->resok->fingers[0].x = fingers[0].first.n;
  res->resok->fingers[0].r = locations->getaddress (fingers[0].first.n);
  n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!fingers[i].first.alive) continue;
    if (fingers[i].first.n != fingers[i-1].first.n) {
      res->resok->fingers[n].x = fingers[i].first.n;
      res->resok->fingers[n].r = locations->getaddress (fingers[i].first.n);
      n++;
    }
  }
}

void
finger_table::fill_getfingersresext (chord_getfingers_ext_res *res)
{

  int n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!fingers[i].first.alive) continue;
    if (fingers[i].first.n != fingers[i-1].first.n) {
      n++;
    }
  }
  res->resok->fingers.setsize (n);
  res->resok->fingers[0].x = fingers[0].first.n;
  location *l = locations->getlocation (fingers[0].first.n);
  res->resok->fingers[0].r = locations->getaddress (fingers[0].first.n);
  res->resok->fingers[0].a_lat = (long)(l->a_lat * 100);
  res->resok->fingers[0].a_var = (long)(l->a_var * 100);
  res->resok->fingers[0].nrpc = l->nrpc;
  res->resok->fingers[0].alive = true;
  n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!fingers[i].first.alive) continue;
    if (fingers[i].first.n != fingers[i-1].first.n) {
      l = locations->getlocation (fingers[i].first.n);
      res->resok->fingers[n].x = fingers[i].first.n;
      res->resok->fingers[n].r = locations->getaddress (fingers[i].first.n);
      res->resok->fingers[n].a_lat = (long)(l->a_lat * 100);
      res->resok->fingers[n].a_var = (long)(l->a_var * 100);
      res->resok->fingers[n].nrpc = l->nrpc;
      res->resok->fingers[n].alive = true;
      n++;
    }
  }
}

void
finger_table::stats ()
{
  warnx << "# fast check in stabilize of finger table " << nfastfinger << "\n";
  warnx << "# slow findsuccessor in stabilize of finger table " << 
    nslowfinger << "\n";
}


void
finger_table::stabilize_succ ()
{
  if (!succ_alive ()) {   // notify() may result in failure
    replacefinger (1);
    chordID s = succ ();
    myvnode->notify (s, myID); 
  }
  nout_continuous++;
  myvnode->get_predecessor
    (succ (), wrap (this,
		    &finger_table::stabilize_getpred_cb, succ ()));
}

void
finger_table::stabilize_getpred_cb (chordID sd,
				    chordID p, net_address r, chordstat status)
{
  // receive predecessor from my successor; in stable case it is me
  if (status) {
    warnx << myID << ": stabilize_getpred_cb " << sd
	  << " failure " << status << "\n";
    stable_fingers = false;
    if (status == CHORD_RPCFAILURE)
      myvnode->deletefingers (sd);
    nout_continuous--;
  } else {
    if (better_ith_finger (1, p)) {
      locations->cacheloc
	(p, r, wrap (this, &finger_table::stabilize_getpred_cb_ok, sd));
    } else {
      nout_continuous--;

      // Shouldn't it be the case that if we ask our successor a
      // question (in this case, for its predecessor), then it already
      // knowns about us -- ie. there's no need to notify it??
      // --josh
      myvnode->notify (sd, myID);
    }
  }
}

void
finger_table::stabilize_getpred_cb_ok (chordID sd,
				       chordID p, bool ok, chordstat status)
{
  nout_continuous--;
  if ((status == CHORD_OK) && ok) {
    if (better_ith_finger (1, p)) {
      updatefinger (p);
      myvnode->notify (sd, myID);
    }
  }
}


//
// This routine tries to keep as finger table entries pointing at the
// optimal chord nodes, ie. those closest to the power-of-2 divisions
// around the ring.  It's impossible to achieve global optimality
// because a chord node cannot omniscently and in real-time see when
// other nodes join and leave the system.  Instead, the finger table
// should be optimal w.r.t. the joins and leaves this node has
// observed.
//
// Consider the case where nodes never leave:
//    - new nodes are offered to all fingers by calling
//    updatefinger().  therefore all fingers are guaranteed to be
//    optimal.
//
// Now consider the actions taken when a node dies:
//   - when node x dies, exactly those finger table entries that
//   pointed at it should be changed to point at the next closest
//   known node succeeding x, ie. closestsucc(x).  This is the optimal 
//   thing to do.  (XXX this is not what is currently done XXX)
//
// Note: the result of this is that fingers are never dead.

//
// Now the proceeding text describes what the optimal steps to take
// are given the information we've observed.  But there are active 
// steps taken as well to keep fingers up-to-date, ie. to approach
// the omniscent global optimality... these are...

void
finger_table::stabilize_finger ()
{
  int i = f % (NBIT+1);

  if (i == 0) {
    stable_fingers2 = stable_fingers;
    stable_fingers = true;
  }

  if (i <= 1) i = 2;		// skip myself and immediate successor

  if (!alive (i)) {
    warnx << "finger_table::stabilize_finger: replace finger " << i << "\n" ;
    replacefinger (i);
    stable_fingers = false;
  }

  if (i <= NBIT) {
    nout_backoff++;
    if (alive (i)) {
      //        warnx << "stabilize: " << myID << " check finger " << i << "\n";
      chordID n = fingers[i].first.n;
      myvnode->get_predecessor
	(n, wrap (this, &finger_table::stabilize_finger_getpred_cb, 
		  n, i));
    } else {
      warnx << "stabilize: " << myID << " findsucc of finger " << i << "\n";
      chordID n = start (i);
      myvnode->find_successor
	(n, wrap (this, &finger_table::stabilize_findsucc_cb,
		  n, i));
    }
    i++;
  }

  f = i;
}

void
finger_table::stabilize_finger_getpred_cb (chordID dn, int i, chordID p, 
					   net_address r, chordstat status)
{
  nout_backoff--;
  if (status) {
    warnx << myID << ": stabilize_finger_getpred_cb: " 
	  << dn << " failure " << status << "\n";
    stable_fingers = false;
    if (status == CHORD_RPCFAILURE)
      myvnode->deletefingers (dn);
  } else {
    chordID n = fingers[i].first.n;
    chordID s = start (i);
    if (betweenrightincl (p, n, s)) {
      //      warnx << "stabilize_finger_getpred_cb: " << myID << " success " 
      //    << i << "\n";
      nfastfinger++;
    } else {
      nslowfinger++;
      //      warnx << "stabilize_finger_getpred_cb: " << myID << " findsucc of finger "
      //    << i << "\n";
      chordID n = start (i);
      nout_backoff++;
      myvnode->find_successor
	(n, wrap (this, &finger_table::stabilize_findsucc_cb, n, i));
    }
  }
}

void
finger_table::stabilize_findsucc_cb (chordID dn, int i, chordID s,
				     route search_path, 
				     chordstat status)
{
  nout_backoff--;
  if (status) {
    warnx << myID << ": stabilize_findsucc_cb: "
	  << dn << " failure " << status << "\n";
    stable_fingers = false;
    if (status == CHORD_RPCFAILURE)
      myvnode->deletefingers (dn);
  } else {
    if (better_ith_finger (i, s)) {
      updatefinger (s);
    }
  }
}
