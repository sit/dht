#include "chord.h"

debruijn::debruijn (ptr<vnode> v,
		  ptr<locationtable> locs,
		  chordID ID)
  : myvnode (v), locations (locs), myID (ID)
{
  mydoubleID = doubleID (myID);
  warn << myID << " debruijn: double " << mydoubleID << "\n";
  locations->pinsucc (mydoubleID);
}


chordID
debruijn::closestsucc (const chordID &x)
{
  chordID s = locations->closestsuccloc (mydoubleID);
  return s;
}

chordID
debruijn::closestpred (const chordID &x)
{
  chordID s = locations->closestpredloc (mydoubleID);
  return s;
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
	    << " : succ " << locations->closestsuccloc (mydoubleID) << "\n";
}
