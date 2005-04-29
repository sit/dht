#include "chord.h"
#include "finger_table.h"
#include <id_utils.h>
#include <misc_utils.h>
#include <location.h>
#include <locationtable.h>
#include <modlogger.h>

#define warning modlogger ("finger_table", modlogger::WARNING)
#define info    modlogger ("finger_table", modlogger::INFO)
#define trace   modlogger ("finger_table", modlogger::TRACE)

ptr<finger_table> 
finger_table::produce_finger_table (ptr<vnode> v, ptr<locationtable> l)
{
  return New refcounted<finger_table> (v, l);
}

// fingers are now zero-indexed!
finger_table::finger_table (ptr<vnode> v, ptr<locationtable> l)
  : myvnode (v),
    locations (l),
    f (0),
    stable_fingers (false),
    stable_fingers2 (false),
    nout_backoff (0),
    nslowfinger (0),
    nfastfinger (0)
{
  myID = v->my_ID ();
  
  for (int i = 0; i < NBIT; i++) {
    starts[i] = successorID (myID, i);
    fingers[i] = NULL;
    locations->pin (starts[i], 1);
    // locations->pin (starts[i], "nsucc"); // XXX look up nsucc from config
  }
}

ptr<location>
finger_table::finger (int i)
{
  // adjusts in "real time"
  return locations->closestsuccloc (starts[i]);
}

ptr<location>
finger_table::operator[] (int i)
{
  return finger (i);
}

ptr<location>
finger_table::closestpred (const chordID &x, vec<chordID> failed)
{
  ptr<location> n;

  for (int i = NBIT - 1; i >= 0; i--) {
    n = finger (i);
    if (between (myID, x, n->id ()) && (!in_vector (failed, n->id ())))
      return n;
  }
  // warn << "no good fingers, returning myID = " << myID << "\n";
  return myvnode->my_location ();
}


void
finger_table::print (strbuf &outbuf)
{
  ptr<location> curfinger = myvnode->my_location ();
  ptr<location> prevfinger = curfinger;
  for (int i = 0; i < NBIT; i++) {
    curfinger = finger (i);
    if (curfinger != prevfinger) {
      outbuf << myID << ": finger: " << i << " : " << starts[i]
	    << " : succ " << curfinger->id () << "\n";
      prevfinger = curfinger;
    }
  }
}

void
finger_table::fill_nodelistres (chord_nodelistres *res)
{
  vec<ptr<location> > fs = get_fingers ();
  res->resok->nlist.setsize (fs.size () + 1);

  myvnode->my_location ()->fill_node (res->resok->nlist[0]);
  for (size_t i = 1; i <= fs.size (); i++)
    fs[i-1]->fill_node (res->resok->nlist[i]);
}

void
finger_table::fill_nodelistresext (chord_nodelistextres *res)
{
  vec<ptr<location> > fs = get_fingers ();
  res->resok->nlist.setsize (fs.size () + 1);

  myvnode->my_location ()->fill_node_ext (res->resok->nlist[0]);
  for (size_t i = 1; i <= fs.size (); i++)
    fs[i-1]->fill_node_ext (res->resok->nlist[i]);
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
    // trace << myID << ": stabilize_finger i = " << i << " f = " << f << "\n";
    stable_fingers2 = stable_fingers;
    stable_fingers = true;
    i = 1; // leave the successor handling to stabilize_succ.
  }
  f = i;
  
  // For each node that we consider, if it has changed since the last time
  // we looked, do the full work to find out who is right. Otherwise,
  // just quickly check to see if its predecessor has changed.
  nout_backoff++;
  if (fingers[i] != finger_table::finger (i)) {
    fingers[i] = finger_table::finger (i);
    stable_fingers = false;
    // Now go forth and find the real finger
    trace << myID << ": stabilize_finger: findsucc of finger " << i << "\n";
    chordID n = start (i);
    myvnode->find_successor
      (n, wrap (this, &finger_table::stabilize_findsucc_cb, n, i));
  } else {
    // trace << myID << ": stabilize_finger: check finger " << i << "\n";    
    ptr<location> n = fingers[i];
    myvnode->get_predecessor
      (n, wrap (this, &finger_table::stabilize_finger_getpred_cb, 
		n->id (), i));
  }  
  // Update f in one of the above callbacks.
}

void
finger_table::stabilize_finger_getpred_cb (chordID dn, int i, chord_node p,
					   chordstat status)
{
  nout_backoff--;
  if (status) {
    warnx << myID << ": stabilize_finger_getpred_cb: " 
	  << dn << " failure status " << status << "\n";
    stable_fingers = false;
    // Do not update f; next round we'll fix this finger correctly.
  } else {
    chordID s = start (i);
    if (betweenrightincl (p.x, dn, s)) {
      // trace << myID << ": stabilize_finger_getpred_cb: success at " 
      //       << i << "\n";
      // predecessor is too far back, no need to do anything for this
      // or any of the other fingers for which n is the successor
      nfastfinger++;
      while (i < NBIT && finger_table::finger (i)->id () == dn)
	i++;
      f = i;
    } else {
      // our finger is wrong; better fix it
      trace << myID << ": stabilize_finger_getpred_cb: "
	    << "fixing finger " << i << "\n";
      nslowfinger++;
      nout_backoff++;
      myvnode->find_successor
	(s, wrap (this, &finger_table::stabilize_findsucc_cb, s, i));
    }
  }
}

void
finger_table::stabilize_findsucc_cb (chordID dn, int i,
				     vec<chord_node> succs,
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
    while (i < NBIT && finger_table::finger (i)->id () == succs[0].x)
      i++;
    f = i;
  }
}

vec<ptr<location> >
finger_table::get_fingers ()
{
  vec<ptr<location> > fs;
  
  ptr<location> curfinger = myvnode->my_location ();
  ptr<location> prevfinger = curfinger;

  for (int i = 0; i < NBIT; i++) {
    curfinger = finger (i);
    if (curfinger != prevfinger) {
      fs.push_back (curfinger);
      prevfinger = curfinger;
    }
  }
  return fs;  
}
