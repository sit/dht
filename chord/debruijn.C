#include "chord.h"

debruijn::debruijn (ptr<vnode> v,
		  ptr<locationtable> locs,
		  chordID ID)
  : myvnode (v), locations (locs), myID (ID)
{
  mydoubleID = doubleID (myID);
  warn << myID << " debruijn: double " << mydoubleID << "\n";
  locs->pinsucc (mydoubleID);
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
