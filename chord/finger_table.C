#include "chord.h"

// fingers are now zero-indexed!
finger_table::finger_table (ptr<vnode> v,
			    ptr<locationtable> locs,
			    chordID ID)
  : myvnode (v), locations (locs), myID (ID) 
{
  for (int i = 0; i < NBIT; i++) {
    starts[i] = successorID (myID, i);
    fingers[i] = myID;
    locations->pinsucc (starts[i]);
  }

  f = 0;
  stable_fingers = false;
  stable_fingers2 = false;
  nout_backoff = 0;

  nslowfinger = 0;
  nfastfinger = 0;
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

chordID
finger_table::closestsuccfinger (chordID &x)
{
  chordID s = x;
  chordID f;
  for (int i = 0; i < NBIT; i++) {
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
  for (int i = NBIT - 1; i >= 0; i--) {
    f = finger (i);
    if (between (myID, x, f)) {
      p = f;
      return p;
    }
  }

  return p;
}

void
finger_table::print ()
{
  chordID curfinger = myID;
  chordID prevfinger = myID;
  for (int i = 0; i < NBIT; i++) {
    curfinger = finger (i);
    if (curfinger != prevfinger) {
      warnx << myID << ": finger: " << i << " : " << starts[i]
	    << " : succ " << finger (i) << "\n";
      prevfinger = curfinger;
    }
  }
}

void
finger_table::fill_getfingersres (chord_getfingersres *res)
{
  int n = 1; // number of valid entries in curfingers
  chordID curfingers[NBIT + 1]; // current unique fingers (plus me)
  chordID curfinger = myID;
  chordID prevfinger = myID;

  curfingers[0] = myID;
  for (int i = 1; i <= NBIT; i++) {
    curfinger = finger (i - 1);
    if (curfinger != prevfinger) {
      curfingers[n] = curfinger;
      prevfinger = curfinger;
      n++;
    }
  }
  
  res->resok->fingers.setsize (n);
  for (int i = 0; i < n; i++) {
    res->resok->fingers[i].x = curfingers[i];
    res->resok->fingers[i].r = locations->getaddress (curfingers[i]);
  }
}

void
finger_table::fill_getfingersresext (chord_getfingers_ext_res *res)
{
  // XXX code duplication with fill_getfingersres
  int n = 1; // number of valid entries in curfingers
  chordID curfingers[NBIT + 1]; // current unique fingers (plus me)
  chordID curfinger = myID;
  chordID prevfinger = myID;

  curfingers[0] = myID;
  for (int i = 1; i <= NBIT; i++) {
    curfinger = finger (i - 1);
    if (curfinger != prevfinger) {
      curfingers[n] = curfinger;
      prevfinger = curfinger;
      n++;
    }
  }
    
  res->resok->fingers.setsize (n);
  for (int i = 0; i < n; i++)
    locations->fill_getnodeext (res->resok->fingers[i], curfingers[i]);
}

void
finger_table::stats ()
{
  warnx << "# fast check in stabilize of finger table " << nfastfinger << "\n";
  warnx << "# slow findsuccessor in stabilize of finger table " << 
    nslowfinger << "\n";
}

// Calls to operator[] will always return the best known successor for
// a given finger at the time the call is made. That is, the finger
// table will be optimal for the joins and leaves that the
// locationtable is aware of. Thus, this routine tries to ensure that
// the locationtable always has good nodes by exploring the regions
// around where the fingers are.
void
finger_table::stabilize_finger ()
{
  int i = f % NBIT;
  
  if (i == 0) {
    // warnx << myID << ": stabilize_finger i = " << i << " f = " << f << "\n";
    stable_fingers2 = stable_fingers;
    stable_fingers = true;
    i = 1; // leave the successor handling to stabilize_succ.
  }
  f = i;
  
  // For each node that we consider, if it has changed since the last time
  // we looked, do the full work to find out who is right. Otherwise,
  // just quickly check to see if its predecessor has changed.
  nout_backoff++;
  if (fingers[i] != finger (i)) {
    fingers[i] = finger (i);
    stable_fingers = false;
    // Now go forth and find the real finger
    warnx << myID << ": stabilize_finger: findsucc of finger " << i << "\n";
    chordID n = start (i);
    myvnode->find_successor
      (n, wrap (this, &finger_table::stabilize_findsucc_cb, n, i));
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
    // Do not update f; next round we'll fix this finger correctly.
  } else {
    chordID s = start (i);
    if (betweenrightincl (p, dn, s)) {
      // warnx << myID << ": stabilize_finger_getpred_cb: success at " 
      //       << i << "\n";
      // predecessor is too far back, no need to do anything for this
      // or any of the other fingers for which n is the successor
      nfastfinger++;
      while (i < NBIT && finger (i) == dn)
	i++;
      f = i;
    } else {
      // our finger is wrong; better fix it
      warnx << myID << ": stabilize_finger_getpred_cb: "
	    << "fixing finger " << i << "\n";
      nslowfinger++;
      nout_backoff++;
      myvnode->find_successor
	(s, wrap (this, &finger_table::stabilize_findsucc_cb, s, i));
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
    // Next round, check this finger again.
  } else {
    while (i < NBIT && finger (i) == s)
      i++;
    f = i;
  }
}

