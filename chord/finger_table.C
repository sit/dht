#include "chord.h"

//#define FINGER_TABLE_CHECK



finger_table::finger_table (ptr<vnode> v,
			    ptr<locationtable> locs,
			    chordID ID)
  : myvnode (v), locations (locs), myID (ID) 
{
  starts[0] = myID;
  fingers[0] = myID;
  locations->increfcnt (myID);

  for (int i = 1; i <= NBIT; i++) {
    starts[i] = successorID (myID, i-1);
    fingers[i] = myID;
    locations->increfcnt (myID);
  }

  f = 0;
  stable_fingers = false;
  stable_fingers2 = false;
  nout_backoff = 0;

  nslowfinger = 0;
  nfastfinger = 0;
}

bool
finger_table::alive (int i)
{
  return locations->alive (fingers[i]);
}

chordID
finger_table::finger (int i)
{
  // adjusts in "real time"
  return locations->closestsuccloc (starts[i]);
}

chordID
finger_table::operator[] (int i)
{
  return finger (i);
}

bool
finger_table::better_ith_finger (int i, chordID s)
{
  if (!alive (i))
    return true;
  else
    return betweenleftincl (starts[i], fingers[i], s);
}

void
finger_table::updatefinger (chordID &x)
{
  check ();
  for (int i = 1; i <= NBIT; i++) {
    if (better_ith_finger (i, x)) {
      locations->decrefcnt (fingers[i]);
      fingers[i] = x;
      locations->increfcnt (fingers[i]);

      // xxx This check should be in succ_list
      if (1 == i)
	myvnode->notify (x, myID);
      else
	stable_fingers = false;
    }
  }
  check ();
}

void
finger_table::replacefinger (int i)
{  
  check ();
  warnx << myID << ": replace finger " << i << "\n" ;
  if (i > 1)
    stable_fingers = false;
  // xxx should we decrement refcnt for old one?
  locations->decrefcnt (fingers[i]);
  fingers[i] = finger (i);
  locations->increfcnt (fingers[i]);
  check ();
}

void 
finger_table::deletefinger (chordID &x)
{
  if (x == myID) return;
  
  check ();
  for (int i = 0; i <= NBIT; i++) {
    if (x == fingers[i])
      replacefinger (i);
  }
  check ();
}

chordID
finger_table::closestsuccfinger (chordID &x)
{
  chordID s = x;
  chordID f;
  for (int i = 0; i <= NBIT; i++) {
    f = finger (i);
    if ((s == x) || between (x, s, f)) {
      s = f;
    }
  }

  return s;
}

chordID 
finger_table::closestpredfinger (chordID &x)
{
  chordID p = myID;
  chordID f;
  for (int i = NBIT; i >= 1; i--) {
    f = finger (i);
    if (between (myID, x, f)) {
      p = f;
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
    if (x == fingers[i])
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

  // Check to see if I'm the only one around.
  for (i = 1; i <= NBIT; i++) {
    if (fingers[i] == myID) continue;
    else break;
  }
  if (i > NBIT) return;

  // Make sure that fingers are in order and non-overlapping.
  for (i = 1; i <= NBIT; i++) {
    // Find the next living finger and...
    j = i+1;
    while ((j <= NBIT) && !alive (j)) j++;
    if (j > NBIT) {
      // if none exists, make sure that it is not after me
      if (!betweenrightincl (starts[i], myID, 
                             fingers[i])) {
	warnx << myID << ": finger_table " << i << " bad\n";
	warnx << myID << ": start " << starts[i] << "\n";
	warnx << myID << ": first " << fingers[i] << "\n";

        print ();
        assert (0);
      }
    } else {
      if (fingers[j] == myID) {
        return;
      }
      if (!betweenrightincl (starts[i], fingers[j], fingers[i])) {
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
    if (!alive (i)) continue;
    warnx << myID << ": finger: " << i << " : " << starts[i]
	  << " : succ " << fingers[i] << "\n";
  }
}

void
finger_table::fill_getfingersres (chord_getfingersres *res)
{
  int n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!alive (i)) continue;
    if (fingers[i] != fingers[i-1]) {
      n++;
    }
  }
  res->resok->fingers.setsize (n);
  res->resok->fingers[0].x = fingers[0];
  res->resok->fingers[0].r = locations->getaddress (fingers[0]);
  n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!alive (i)) continue;
    if (fingers[i] != fingers[i-1]) {
      res->resok->fingers[n].x = fingers[i];
      res->resok->fingers[n].r = locations->getaddress (fingers[i]);
      n++;
    }
  }
}

void
finger_table::fill_getfingersresext (chord_getfingers_ext_res *res)
{

  int n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!alive (i)) continue;
    if (fingers[i] != fingers[i-1]) {
      n++;
    }
  }
  res->resok->fingers.setsize (n);
  location *l = locations->getlocation (fingers[0]);
  res->resok->fingers[0].x = fingers[0];
  res->resok->fingers[0].r = locations->getaddress (fingers[0]);
  res->resok->fingers[0].a_lat = (long)(l->a_lat * 100);
  res->resok->fingers[0].a_var = (long)(l->a_var * 100);
  res->resok->fingers[0].nrpc = l->nrpc;
  res->resok->fingers[0].alive = true;
  n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!alive (i)) continue;
    if (fingers[i] != fingers[i-1]) {
      l = locations->getlocation (fingers[i]);
      res->resok->fingers[n].x = fingers[i];
      res->resok->fingers[n].r = locations->getaddress (fingers[i]);
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

int
finger_table::runlength (int i)
{
  int runlen = 1;
  while ((i + runlen < NBIT + 1) &&
	 fingers[i + runlen] == fingers[i])
    runlen++;
  return runlen;
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
    // warnx << myID << ": stabilize_finger i = " << i << " f = " << f << "\n";
    stable_fingers2 = stable_fingers;
    stable_fingers = true;
  }
  f = i;

  if (i <= 1) i = 2;		// skip myself and immediate successor

  nout_backoff++;
  if (!alive (i)) {
    // Find some sort of temporary replacement.
    replacefinger (i);
    updatefinger (fingers[i]);

    // Now go forth and find the real finger
    warnx << myID << ": stabilize_finger: findsucc of finger " << i << "\n";
    chordID n = start (i);
    myvnode->find_successor
      (n, wrap (this, &finger_table::stabilize_findsucc_cb,
  		n, i));
  } else {
    // warnx << myID << ": stabilize_finger: check finger " << i << "\n";    
    chordID n = fingers[i];
    myvnode->get_predecessor
      (n, wrap (this, &finger_table::stabilize_finger_getpred_cb, 
		n, i));
  }
  
  // Update f in one of the above callbacks.
}

void
finger_table::stabilize_finger_getpred_cb (chordID dn, int i, chordID p, 
					   net_address r, chordstat status)
{
  nout_backoff--;
  if (status) {
    warnx << myID << ": stabilize_finger_getpred_cb: " 
	  << dn << " failure status " << status << "\n";
    stable_fingers = false;
    if (status == CHORD_RPCFAILURE)
      deletefinger (dn);
    // Do not update f; next round we'll fix this finger correctly.
  } else {
    chordID n = fingers[i];
    chordID s = start (i);
    if (betweenrightincl (p, n, s)) {
      // warnx << myID << ": stabilize_finger_getpred_cb: success at " 
      //       << i << "\n";
      // predecessor is too far back, no need to do anything for this
      // or any of the other fingers for which n is the successor
      nfastfinger++;
      f += runlength (i);
    } else {
      // our finger is wrong; better fix it
      warnx << myID << ": stabilize_finger_getpred_cb: "
	    << "fixing finger " << i << "\n";
      nslowfinger++;
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
	  << dn << " failure status " << status << "\n";
    stable_fingers = false;
    if (status == CHORD_RPCFAILURE)
      deletefinger (dn);
  } else {
    updatefinger (s);
    f += runlength (i);
  }
}
