#include "chord.h"

debruijn::debruijn (ptr<vnode> v,
		  ptr<locationtable> locs,
		  chordID ID)
  : myvnode (v), locations (locs), myID (ID)
{
  for (int i=0; i < LOGBASE; i++) 
    mydoubleID[i] = doubleID (myID, i+1);
  for (int i=0; i < LOGBASE; i++) 
    locations->pinsucc (mydoubleID[i]);
  warn << myID << " debruijn: double " << mydoubleID[0] << "\n";
}


chordID
debruijn::closestsucc (const chordID &x)
{
  chordID s = myID;
  chordID n;
  chordID pred = myvnode->my_pred ();

  if (betweenrightincl (myID, pred, x)) s = pred;
  else s = myID;

  for (int i = 0; i < LOGBASE; i++) {
    n = locations->closestsuccloc (mydoubleID[i]);
    if (betweenrightincl (myID, n, x) && between (x, s, n)) {
      s = n;
    }
  }
  return s;
}

chordID
debruijn::closestpred (const chordID &x)
{
  chordID p;
  chordID n;
  chordID pred = myvnode->my_pred ();

  if (between (myID, x, pred)) p = pred;
  else p = myID;

  for (int i = 0; i < LOGBASE; i++) {
    n = locations->closestsuccloc (mydoubleID[i]);
    if (between (myID, x, n) && between (p, x, n))
      p = n;
  }
  return p;
}

void
debruijn::stabilize ()
{
  for (int i = 0; i < LOGBASE; i++) 
    myvnode->find_successor (mydoubleID[0], 
			   wrap (this, &debruijn::finddoublesucc_cb));
}

void
debruijn::finddoublesucc_cb (chordID s, route search_path, chordstat status)
{
  if (status) {   
    warnx << myID << ": finddoublesucc_cb: failure status " << status << "\n";
  } else {
    //  warnx << myID << ": finddoublesucc_cb: " << mydoubleID << " is " << s 
    //  << "\n";
  }
}

void
debruijn::print ()
{
  for (int i = 0; i < LOGBASE; i++) {
    warnx << myID << ": double: " << mydoubleID[i]
	  << " : succ " << locations->closestsuccloc (mydoubleID[i]) << "\n";
  }
}
