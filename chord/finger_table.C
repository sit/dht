#include "chord.h"

// fingers are now zero-indexed!
finger_table::finger_table ()  
{

  f = 0;
  stable_fingers = false;
  stable_fingers2 = false;
  nout_backoff = 0;

  nslowfinger = 0;
  nfastfinger = 0;
}

void 
finger_table::init (ptr<vnode> v, ptr<locationtable> locs, chordID ID)
{
  locations  = locs;
  myvnode = v;
  myID = ID;
  
  for (int i = 0; i < NBIT; i++) {
    starts[i] = successorID (myID, i);
    fingers[i] = myID;
    locations->pinsucc (starts[i]);
  }

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
finger_table::closestsucc (const chordID &x)
{
  chordID n;

  for (int i = 0; i < NBIT; i++) {
    n = finger (i);
    if (between (myID, n, x))
      return n;
  }
  return myID;
}

chordID
finger_table::closestpred (const chordID &x, vec<chordID> failed)
{
  chordID n;

  for (int i = NBIT - 1; i >= 0; i--) {
    n = finger (i);
    if (between (myID, x, n) && (!in_vector (failed, n)))
      return n;
  }
  warn << "no good fingers, returning myID = " << myID << "\n";
  return myID;
}


chordID
finger_table::closestpred (const chordID &x)
{
  chordID n;

  for (int i = NBIT - 1; i >= 0; i--) {
    n = finger (i);
    if (between (myID, x, n))
      return n;
  }
  return myID;
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
finger_table::fill_nodelistres (chord_nodelistres *res)
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
  
  res->resok->nlist.setsize (n);
  for (int i = 0; i < n; i++) {
    res->resok->nlist[i].x = curfingers[i];
    res->resok->nlist[i].r = locations->getaddress (curfingers[i]);
  }
}

void
finger_table::fill_nodelistresext (chord_nodelistextres *res)
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
    
  res->resok->nlist.setsize (n);
  for (int i = 0; i < n; i++)
    locations->fill_getnodeext (res->resok->nlist[i], curfingers[i]);
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

