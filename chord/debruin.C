#include "chord.h"

debruin::debruin (ptr<vnode> v,
		  ptr<locationtable> locs,
		  chordID ID)
  : myvnode (v), locations (locs), myID (ID)
{
  mydoubleID = doubleID (myID);
  warn << myID << " debruin: double " << mydoubleID << "\n";
}

void
debruin::stabilize ()
{
  myvnode->find_successor (mydoubleID, 
			   wrap (this, &debruin::finddoublesucc_cb));
}

void
debruin::finddoublesucc_cb (chordID s, route search_path, chordstat status)
{
  if (status) {   
    warnx << myID << ": finddoublesucc_cb: failure status " << status << "\n";
  } else {
    //  warnx << myID << ": finddoublesucc_cb: " << mydoubleID << " is " << s 
    //  << "\n";
  }
}
