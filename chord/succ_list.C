#include "chord.h"

/*
 * The idea of the succ_list class is to maintain a list of
 * immediate successors to the current node. The actual number
 * of successors needed is up to NSUCC. The location information
 * is maintained in locationtable.
 */
succ_list::succ_list (ptr<vnode> v,
		      ptr<locationtable> locs,
		      chordID ID)
  : myID (ID), myvnode (v), locations (locs)
{
  nsucc = 0;
  nnodes = 0;

  s = 0;
  oldsucc = myID - 1;
  nout_backoff = 0;
  nout_continuous = 0;
  stable_succlist = false;
  stable_succlist2 = false;
}
   
chordID 
succ_list::succ ()
{
  return locations->closestsuccloc (myID);
}

int
succ_list::num_succ ()
{
  int goodnodes = locations->usablenodes () - 1;
  int newnsucc = (NSUCC > goodnodes) ? goodnodes : NSUCC;
  return newnsucc;
}

void
succ_list::print ()
{
  chordID id = myID;
  for (int i = 0; i < num_succ (); i++) {
    id = locations->closestsuccloc (id);
    warnx << myID << ": succ " << i + 1 << " : " << id << "\n";
  }
}

u_long
succ_list::estimate_nnodes ()
{
  u_long n;
  chordID lastsucc = myID;
  for (int i = 0; i < nsucc; i++)
    lastsucc = locations->closestsuccloc (lastsucc);
  chordID d = diff (myID, lastsucc);
  if ((d > 0) && (nsucc > 0)) {
    chordID s = d / nsucc;
    chordID c = bigint (1) << NBIT;
    chordID q = c / s;
    n = q.getui ();
  } else 
    n = 1;
  return n;
}

void
succ_list::fill_getsuccres (chord_getsucc_ext_res *res)
{
  // succ[0] is me. the rest are our actual successors.
  int curnsucc = num_succ ();
  chordID cursucc = myID;
  res->resok->succ.setsize (curnsucc + 1);
  for (int i = 0; i <= curnsucc; i++) {
    locations->fill_getnodeext (res->resok->succ[i], cursucc);
    cursucc = locations->closestsuccloc (cursucc);
  }
}

void
succ_list::stabilize_succlist ()
{
  assert (nout_backoff == 0);
  int next = s % (nsucc + 1); // probe past the end.
  
  // warnx << myID << ": stabilize_succlist next = " << next
  //       << " nsucc = " << nsucc << "\n";
  if (next == 0) {
    stable_succlist2 = stable_succlist;
    stable_succlist = true;
    
    int goodnodes = locations->usablenodes () - 1;
    int newnsucc = (NSUCC > goodnodes) ? goodnodes : NSUCC;
    warnx << myID << ": estimating total number of nodes as "
	  << locations->estimate_nodes () << ";"
    // warnx << myID << ": stabilize_succlist newnsucc = " << newnsucc
	  << " goodnodes = " << goodnodes << "\n";

    // wait until we've discovered some other node via cts process.
    if (newnsucc == 0)
      return;

    // At each round, just walk through our list, skipping the first.
    nextsucc = succ ();
    next = 1;

    nsucc = newnsucc;
  }
  
  if (next < nsucc + 1) { // in case we've gotten really small
    chordID expected = locations->closestsuccloc (nextsucc);
    nout_backoff++;
    myvnode->get_successor
      (nextsucc, wrap (this, &succ_list::stabilize_getsucc_cb,
		       expected, next));
  } else {
    s = 0;
  }
  // Update s in the callbacks.
}


void
succ_list::stabilize_getsucc_cb (chordID expected, int j,
				 chordID succ, net_address r, 
				 chordstat status)
{
  // the person we asked in nextsucc
  nout_backoff--;
  if (status) {
    warnx << myID << ": stabilize_getsucc_cb: " << j << " : " 
	  << expected << " failure " << status << "\n";
    stable_succlist = false;
    s = 0;
  } else if (j <= nsucc) {
    if (succ == myID) {
      if (j != nsucc)
	warnx << myID << ": stabilize_getsucc_cb full circle; nsucc = "
	      << nsucc << " j = " << j << "\n";
      s = 0;
    } else if (expected != succ) {
      nout_backoff++;
      stable_succlist = false;
      if (between (nextsucc, expected, succ)) {
	warnx << myID << ": stabilize_getsucc_cb caching new successor "
	      << j << " = " << succ << "\n";
	locations->cacheloc (succ, r,
			     wrap (this, &succ_list::stabilize_getsucc_ok, 
				   j));
      } else {
	// We got someone worse than expected; better check it out.
	warnx << myID << ": got new successor " << succ << " worse than "
	      << expected << "; checking.\n";
	locations->ping (expected,
			 wrap (this, &succ_list::stabilize_getsucc_checkold_cb,
			       j, succ, r));
      }
    } else {
      nextsucc = expected;
      s = j + 1;
    }
    // ring probably changed a lot recently, esp if j != nsucc.
    // better start the whole process over again.
  }
  u_long n = estimate_nnodes ();
  locations->replace_estimate (nnodes, n);
  nnodes = n;
}

void
succ_list::stabilize_getsucc_checkold_cb (int j, chordID succ, net_address r,
					  chordstat status)
{
  // we expect that the old guy is dead.
  if (!status) {
    warnx << myID
	  << ": old successor is alive!\n";
    // xxx do we need to do anything special? notify the node
    //     we got this answer from?
  }
  // There is no harm in caching this new guy anyway though; we
  // will challenge him anyway, and for all we know he's a useful
  // node to keep in mind.
  
  // warnx << myID << ": stabilize_getsucc_checkold_cb caching new successor "
  //	<< j << " = " << succ << "\n";
  locations->cacheloc (succ, r,
		       wrap (this, &succ_list::stabilize_getsucc_ok, 
			     j));
}

void
succ_list::stabilize_getsucc_ok (int j, chordID succ, bool ok, chordstat status)
{
  nout_backoff--;
  if ((status == CHORD_OK) && ok) {
    nextsucc = succ;
    s = j + 1;
    warnx << myID << ": stabilize_getsucc_ok: s incremented to " << s << "\n";
  }
}


// ============
void
succ_list::stabilize_succ ()
{
  chordID cursucc = succ ();
  if (cursucc != oldsucc) {
    warnx << myID << ": my successor changed from "
	  << oldsucc << " to " << cursucc << "\n";
    myvnode->notify (cursucc, myID);
    oldsucc = cursucc;
    // Wait until next round to check on this guy.
  } else {
    nout_continuous++;
    myvnode->get_predecessor
      (cursucc, wrap (this, &succ_list::stabilize_getpred_cb, cursucc));
  }
}

void
succ_list::stabilize_getpred_cb (chordID sd, chordID p, net_address r,
				 chordstat status)
{
  nout_continuous--;
  // receive predecessor from my successor; in stable case it is me
  if (status) {
    warnx << myID << ": stabilize_getpred_cb " << sd
    	  << " failure status " << status << "\n";
    if (status == CHORD_ERRNOENT) {
      warnx << myID << ": stabilize_getpred_cb " << sd
	    << " doesn't know about his predecessor?? notifying...\n";
      myvnode->notify (sd, myID);
    }
    // other failures we will address next round.
  } else {
    if (myID == p) {
      // Good, things are as we expect.
    } else if (betweenleftincl (myID, sd, p)) {
      // Did we get someone strictly better?
      nout_continuous++;
      locations->cacheloc
	(p, r, wrap (this, &succ_list::stabilize_getpred_cb_ok, sd));
    } else {
      // Our successor appears to be confused, better tell
      // him what we think.
      myvnode->notify (sd, myID);
    }
  }
}

void
succ_list::stabilize_getpred_cb_ok (chordID sd,
				    chordID p, bool ok, chordstat status)
{
  nout_continuous--;
  if (ok && (status == CHORD_OK)) {
    oldsucc = p;
  }
}
