#include "chord.h"

/*
 * The idea of the succ_list class is to maintain a list of
 * immediate successors to the current node. The actual number
 * of successors needed is up to NSUCC. The location information
 * is maintained in locationtable; we just track the chordIDs
 * here so that we can tell when things have changed.
 */
 
succ_list::succ_list (ptr<vnode> v,
		      ptr<locationtable> locs,
		      chordID ID)
  : myID (ID), myvnode (v), locations (locs)
{
  succlist[0] = myID;
  locations->increfcnt (myID);
  nsucc = 0;
  nnodes = 0;

  for (int i = 1; i <= NSUCC; i++) {
    succlist[i] = 0;
  }

  s = 0;
  nout_backoff = 0;
  nout_continuous = 0;
  stable_succlist = false;
  stable_succlist2 = false;
}

bool
succ_list::nth_alive (int n)
{
  if (n > nsucc)
    return false;
  return locations->alive (succlist[n]);
}
   
chordID 
succ_list::succ ()
{
  return locations->closestsuccloc (myID);
}

int
succ_list::countrefs (chordID &x)
{
  int n = 0;
  for (int i = 0; i <= nsucc; i++) 
    if (x == succlist[i])
      n++;
  return n;
}

void
succ_list::print ()
{
  for (int i = 1; i <= nsucc; i++) {
    if (!nth_alive (i)) continue;
    warnx << myID << ": succ " << i << " : " << succlist[i] << "\n";
  }
}

void
succ_list::replace_succ (int j)
{
  assert (j > 0);
  // warnx << myID << ": replace succ " << j << "\n";
  if (j > nsucc)
    panic << myID << ": " << j << " > " << nsucc << "\n";

  locations->decrefcnt (succlist[j]);
  succlist[j] = locations->closestsuccloc (succlist[j - 1]);
  locations->increfcnt (succlist[j]);

  if (succlist[j] == myID) {
    if (j < nsucc) { warnx << myID << " lost more than one successor...\n"; }
    for (int i = nsucc; i >= j; i--)
      remove_succ (i);
  } else {
    if (j == 1)
      myvnode->notify (succlist[1], myID);
  }
}

void
succ_list::remove_succ (int j)
{
  if (j > nsucc)
    return;
  stable_succlist = false;
  locations->decrefcnt (succlist[j]);
  
  if (locations->alive (succlist[j]))
    locations->ping (succlist[j], NULL);

  for (int i = j + 1; i <= nsucc; i++) {
    succlist[i - 1] = succlist[i];
  }
  nsucc--;
}

void
succ_list::delete_succ (chordID &x)
{
  for (int i = nsucc; i > 0; i--) {
    if (x == succlist[i]) {
      warnx << myID << ": delete_succ: removing " << i << "\n";
      remove_succ (i);
    }
  }
}

u_long
succ_list::estimate_nnodes ()
{
  u_long n;
  chordID d = diff (myID, succlist[nsucc]);
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
  int n = 1;
  for (int i = 1; i <= nsucc; i++) {
    if (nth_alive (i))
      n++;
  }
  res->resok->succ.setsize (n);
  location *l = locations->getlocation (succlist[0]);
  res->resok->succ[0].x = succlist[0];
  res->resok->succ[0].r = locations->getaddress (succlist[0]);
  res->resok->succ[0].a_lat = (long)(l->a_lat * 100);
  res->resok->succ[0].a_var = (long)(l->a_var * 100);
  res->resok->succ[0].nrpc = l->nrpc;
  res->resok->succ[0].alive = true;
  n = 1;
  for (int i = 1; i <= nsucc; i++) {
    if (!nth_alive (i)) continue;
    l = locations->getlocation (succlist[i]);
    res->resok->succ[n].x = succlist[i];
    res->resok->succ[n].r = locations->getaddress (succlist[i]);
    res->resok->succ[n].a_lat = (long)(l->a_lat * 100);
    res->resok->succ[n].a_var = (long)(l->a_var * 100);
    res->resok->succ[n].nrpc = l->nrpc;
    res->resok->succ[n].alive = true;
    n++;
  }
}

chordID
succ_list::operator[] (int n) 
{ 
  if (n > nsucc) 
    return succlist[nsucc];
  else
    return succlist[n]; 
}

void
succ_list::stabilize_succlist ()
{
  assert (nout_backoff == 0);
  int next = s % (nsucc + 1);
  int change = 0;
  
  // warnx << myID << ": stabilize_succlist next = " << next
  //	<< " nsucc = " << nsucc << "\n";
  if (next == 0) {
    stable_succlist2 = stable_succlist;
    stable_succlist = true;
    
    // At each round, figure out who our current successors are.
    // If there's one that doesn't match, then find his successor.
    // This works in conjunction with the stabilization of the
    // successor which will be continuously stirring the
    // locations in the locationtable.
    int goodnodes = locations->usablenodes () - 1;
    int newnsucc = (NSUCC > goodnodes) ? goodnodes : NSUCC;
    warnx << myID << ": stabilize_succlist newnsucc = " << newnsucc
	  << " goodnodes = " << goodnodes << "\n";

    // wait until we've discovered some other node via cts process.
    if (newnsucc == 0)
      return;

    // Calculate new list of actual successors and replace
    // the old one. Note if there is a change...
    chordID currsuc[NSUCC + 1];
    currsuc[0] = myID;
    for (int j = 1; j <= newnsucc; j++)
      currsuc[j] = locations->closestsuccloc (currsuc[j - 1]);
    
    // Copy/check old ones
    for (int j = 1; j <= nsucc; j++) {
      if (j > newnsucc) break;
      if (currsuc[j] != succlist[j]) {
	locations->decrefcnt (succlist[j]);
	succlist[j] = currsuc[j];
	locations->increfcnt (succlist[j]);
	if (!change) change = j;
      }
    }
    // Copy the new ones.
    for (int j = nsucc + 1; j <= newnsucc; j++) {
      succlist[j] = currsuc[j];
      locations->increfcnt (succlist[j]);
      if (!change) change = j;
    }
    nsucc = newnsucc;
    next = 2; // let continuous handle successor...
  }
  
  if (change) {
    nout_backoff++;
    myvnode->get_successor
      (succlist[change - 1], wrap (this, &succ_list::stabilize_getsucc_cb,
				   succlist[change], change));
  } else if (next <= nsucc) {
    if (locations->alive (succlist[next - 1])) {
      nout_backoff++;
      myvnode->get_successor
	(succlist[next - 1], wrap (this, &succ_list::stabilize_getsucc_cb,
				   succlist[next], next));
    } else {
      s = 0;
    }
  }
  // Update s in the callbacks.
}


void
succ_list::stabilize_getsucc_cb (chordID jid,
				 int j, chordID succ, net_address r, 
				 chordstat status)
{
  nout_backoff--;
  if (status) {
    warnx << myID << ": stabilize_getsucc_cb: " << j << " : " 
	  << jid << " failure " << status << "\n";
    stable_succlist = false;
    if (status == CHORD_RPCFAILURE)
      delete_succ (succlist[j - 1]);
    s = 0;
  } else if (j <= nsucc) {
    if (succ == myID) {
      // warnx << myID << ": stabilize_getsucc_cb full circle; nsucc = "
      //       << nsucc << " j = " << j << "\n";
      for (int i = nsucc; i >= j; i--)
	remove_succ (i);
      s = 0;
    } else if (jid != succ) {
      nout_backoff++;
      if (!between (succlist[j - 1], jid, succ)) {
	locations->ping (jid,
			 wrap (this, &succ_list::stabilize_getsucc_checkold_cb,
			       j, succ, r));
      } else {
	// warnx << myID << ": stabilize_getsucc_cb caching new successor "
	//       << j << " = " << succ << "\n";
	locations->cacheloc (succ, r,
			     wrap (this, &succ_list::stabilize_getsucc_ok, 
				   j));
      }
    } else {
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
    warn << myID
	 << ": got new successor but it's not better and old one alive!\n";
    // xxx
  }
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
    if (succlist[j] != succ) {
      stable_succlist = false;
      locations->decrefcnt (succlist[j]);
      succlist[j] = succ;
      locations->increfcnt (succlist[j]);
      for (int i = j + 1; i <= nsucc; i++)
	replace_succ (i);
    }
    s = j + 1;
    // warnx << myID << ": stabilize_getsucc_ok: s incremented to " << s << "\n";
  }
}


// ============
void
succ_list::stabilize_succ ()
{
  chordID s = succ ();
  if (!nth_alive (1)) {
    if (nsucc < 1) {
      succlist[1] = s;
      locations->increfcnt (succlist[1]);
      myvnode->notify (succlist[1], myID);
    } else {
      replace_succ (1);
    }
    // warnx << myID << ": stabilize_succ dead... new succ is " << s << "\n";
  }
  nout_continuous++;
  myvnode->get_predecessor
    (s, wrap (this, &succ_list::stabilize_getpred_cb, s));
}

void
succ_list::stabilize_getpred_cb (chordID sd, chordID p, net_address r,
				 chordstat status)
{
  // receive predecessor from my successor; in stable case it is me
  if (status) {
    // warnx << myID << ": stabilize_getpred_cb " << sd
    //	  << " failure status " << status << "\n";
    if (status == CHORD_ERRNOENT) {
      // warnx << myID << ": stabilize_getpred_cb " << sd
      //    << " doesn't know about his predecessor?? notifying...\n";
      myvnode->notify (sd, myID);
    } else if (status == CHORD_RPCFAILURE) {
      replace_succ (1);
    }
    nout_continuous--;
  } else {
    if (myID == p) {
      // Why am I talking to myself anyway?
      nout_continuous--;
    } else if (betweenleftincl (myID, sd, p)) {
      // Did we get someone strictly better?
      locations->cacheloc
	(p, r, wrap (this, &succ_list::stabilize_getpred_cb_ok, sd));
    } else {
      nout_continuous--;

      // Shouldn't it be the case that if we ask our successor a
      // question (in this case, for its predecessor), then it already
      // knowns about us -- ie. there's no need to notify it??
      // --josh
      myvnode->notify (sd, myID);
    }
  }
}

void
succ_list::stabilize_getpred_cb_ok (chordID sd,
				    chordID p, bool ok, chordstat status)
{
  nout_continuous--;
  if ((status == CHORD_OK) && ok) {
    replace_succ (1);
  }
}
