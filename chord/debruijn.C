#include "chord.h"

#include "fingerlike.h"
#include "debruijn.h"
#include "location.h"

debruijn::debruijn () {}

void 
debruijn::init (ptr<vnode> v, ptr<locationtable> locs, chordID ID)
{
  myvnode = v;
  locations = locs;
  myID = ID;
  mydoubleID = doubleID (myID, logbase);
  locations->pinpred (mydoubleID);
  warn << myID << " de bruijn: double :" << mydoubleID << "\n";
}

chordID
debruijn::debruijnptr ()
{
  return locations->closestpredloc (mydoubleID);
}

chordID
debruijn::closestsucc (const chordID &x)
{
  chordID s = myID;
  chordID succ = locations->closestsuccloc (myID + 1);
  chordID n;

  if (betweenrightincl (myID, succ, x)) s = succ;
  else s = myID;

  for (int i = 1; i < logbase; i++) {
    n = locations->closestsuccloc (succ + 1);
    if (betweenrightincl (myID, n, x) && between (x, s, n)) {
      s = n;
    }
    succ = n;
  }

  // use closestpred, because we pinned pred
  n = locations->closestpredloc (mydoubleID); 
  if (betweenrightincl (myID, n, x) && between (x, s, n)) {
    s = n;
  }

  return s;
}

//XXX ignores failed node list.
chordID
debruijn::closestpred (const chordID &x, vec<chordID> f)
{
  chordID succ = locations->closestsuccloc (myID + 1);
  chordID p;
  chordID n;

  if (betweenrightincl (myID, succ, x)) p = myID;
  else p = succ;

  for (int i = 1; i < logbase; i++) {
    n = locations->closestsuccloc (succ + 1);
    if (between (myID, x, n) && between (p, x, n)) {
      p = n;
    }
    succ = n;
  }

  // use closestpred, because we pinned pred
  n = locations->closestpredloc (mydoubleID);
  if (between (myID, x, n) && between (p, x, n)) {
    p = n;
  }

  return p;
}

chordID
debruijn::closestpred (const chordID &x)
{
  chordID succ = locations->closestsuccloc (myID + 1);
  chordID p;
  chordID n;

  if (betweenrightincl (myID, succ, x)) p = myID;
  else p = succ;

  for (int i = 1; i < logbase; i++) {
    n = locations->closestsuccloc (succ + 1);
    if (between (myID, x, n) && between (p, x, n)) {
      p = n;
    }
    succ = n;
  }

  // use closestpred, because we pinned pred
  n = locations->closestpredloc (mydoubleID);
  if (between (myID, x, n) && between (p, x, n)) {
    p = n;
  }

  return p;
}

void
debruijn::stabilize ()
{
  myvnode->find_successor (mydoubleID, 
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
  warnx << myID << ": double: " << mydoubleID
	<< " : d " << locations->closestpredloc (mydoubleID) << "\n";
}


class dbiter : public fingerlike_iter {
  friend class debruijn;
public:
  dbiter () : fingerlike_iter () {};
};

ref<fingerlike_iter>
debruijn::get_iter ()
{
  ref<dbiter> iter = New refcounted<dbiter> ();
  // XXX do something useful here.
  return iter;
}
