#include "chord.h"
#include "fingerlike.h"
#include "succ_list.h"
#include "location.h"

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
  nnodes = 0;

  oldsucc = myID;
  stable_succlist = false;
  stable_succlist2 = false;
  nout_backoff = 0;
  nout_continuous = 0;

  locations->pinsucc (myID);
  locations->pinsucclist (myID);
}
   
chordID 
succ_list::succ ()
{
  return locations->closestsuccloc (myID + 1);
}

//succ_list[0] is the immediate successor
chordID
succ_list::operator[] (unsigned int n) 
{
  assert (n < num_succ ());
  chordID ret = succ ();
  for (u_int i = 0; i < n; i++) 
    ret = locations->closestsuccloc (ret + 1);

  return ret;
}

vec<chord_node>
succ_list::succs ()
{
  vec<chord_node> ret;
  chordID cur = succ ();
  chord_node n;

  locations->get_node (cur, &n);
  ret.push_back (n);

  for (u_int i = 1; i < num_succ (); i++) {
    cur = locations->closestsuccloc (cur + 1);
    locations->get_node (cur, &n);
    ret.push_back (n);
  }
  return ret;
}

unsigned int
succ_list::num_succ ()
{
  int goodnodes = locations->usablenodes () - 1;
  int newnsucc = (NSUCC > goodnodes) ? goodnodes : NSUCC;
  
  if (newnsucc < 0) {
    warn << "succ_list::num_succ () n:" << newnsucc << " g:" << goodnodes << "\n";
    newnsucc = 0;
  }
  return newnsucc;
}

void
succ_list::print ()
{
  chordID id = myID;
  for (u_int i = 0; i < num_succ (); i++) {
    id = locations->closestsuccloc (id + 1);
    warnx << myID << ": succ " << i + 1 << " : " << id << "\n";
  }
}

u_long
succ_list::estimate_nnodes ()
{
  u_long n;
  chordID lastsucc = myID;
  int nsucc = num_succ ();
  for (int i = 0; i < nsucc; i++)
    lastsucc = locations->closestsuccloc (lastsucc + 1);
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
succ_list::fill_nodelistresext (chord_nodelistextres *res)
{
  // succ[0] is me. the rest are our actual successors.
  int curnsucc = num_succ ();
  chordID cursucc = myID;
  res->resok->nlist.setsize (curnsucc + 1);
  for (int i = 0; i <= curnsucc; i++) {
    locations->fill_getnodeext (res->resok->nlist[i], cursucc);
    cursucc = locations->closestsuccloc (cursucc + 1);
  }
}

void
succ_list::fill_nodelistres (chord_nodelistres *res)
{
  res->resok->nlist.setsize (0);
}

void
succ_list::stabilize_succlist ()
{
  assert (nout_backoff == 0);
  
  stable_succlist2 = stable_succlist;
  stable_succlist = true;
  
  u_long n = estimate_nnodes ();
  locations->replace_estimate (nnodes, n);
  if (nnodes != n) {
    warnx << myID << ": estimating total number of nodes as "
	  << locations->estimate_nodes () << "\n";
  }
  nnodes = n;

  nout_backoff++;
  chordID s = succ ();
  myvnode->get_succlist
    (s, wrap (this, &succ_list::stabilize_getsucclist_cb, s));
}

void
succ_list::stabilize_getsucclist_cb (chordID s, vec<chord_node> nlist,
				     chordstat status)
{
  nout_backoff--;
  if (status) {
    warnx << myID << ": stabilize_getsucclist_cb: " << s << " : " 
	  << "failure " << status << "\n";
    stable_succlist = false;
    return;
  }
  assert (s == nlist[0].x);

  // PODC paper says, set our succlist to successors succlist mod last guy
  // We're going to merge his succlist with our succlist and make sure
  // deletions and insertions are accurate from our POV.
  unsigned int i, j;
  vec<chordID> succlist;
  unsigned int curnsucc = num_succ ();
  chordID cursucc = succ ();
  for (i = 0; i < curnsucc; i++) {
    succlist.push_back (cursucc);
    cursucc = locations->closestsuccloc (cursucc + 1);
  }

  i = 0; j = 0;
  unsigned int newnsucc = nlist.size () - 1; // drop last guy.
  while ((i < curnsucc) && (j < newnsucc)) {    
    if (succlist[i] == nlist[j].x) { i++; j++; continue; }
    if (between (myID, nlist[j].x, succlist[i])) {
      // if succlist[i] < nlist[j].x
      // then, maybe someone we knew about is dead now. best be sure.
      nout_backoff++;
      myvnode->ping
	(succlist[i], wrap (this, &succ_list::stabilize_getsucclist_check,
			    s, succlist[i]));
      i++;
      continue;
    }
    if (between (myID, succlist[i], nlist[j].x)) {
      // if succlist[i] > nlist[j].x
      // then maybe a new node joined. check it out.
      //BAD LOC (ok)
      bool ok = locations->insert (nlist[j]);
      if (!ok) {
	warnx << myID << ": stabilize_succlist: received bad successor "
	      << nlist[j].x << " from " << s << "\n";
	// XXX do something about it?
      } else {
	stable_succlist = false;
	warnx << myID << ": stabilize_succlist: received new successor "
	      << nlist[j].x << " from " << s << "\n";
      }
      j++;
      continue;
    }
  }
  bool check = false;
  while (i < curnsucc) {
    check = true;
    nout_backoff++;
    myvnode->ping
      (succlist[i], wrap (this, &succ_list::stabilize_getsucclist_check,
			  s, succlist[i]));
    i++;
  }
  while (j < newnsucc) {
    assert (!check);
    //BAD LOC
    bool ok = locations->insert (nlist[j]);
    if (!ok) {
      warnx << myID << ": stabilize_succlist: received bad successor "
	    << nlist[j].x << " from " << s << "\n";
      // XXX do something about it?
    } else {
      stable_succlist = false;
      warnx << myID << ": stabilize_succlist: received new successor "
	    << nlist[j].x << " from " << s << "\n";
    }
    j++;
  }
}

void
succ_list::stabilize_getsucclist_check (chordID src, chordID chk,
					chordstat status)
{
  nout_backoff--;
  if (status) {
    stable_succlist = false;
    warnx << myID << ": stabilize_succlist: found dead successor " << chk
	  << " from " << src << "\n";
  }
}

// ============
void
succ_list::stabilize_succ ()
{
  assert (nout_continuous == 0);
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
succ_list::stabilize_getpred_cb (chordID sd, chord_node p, chordstat status)
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
    if (myID == p.x) {
      // Good, things are as we expect.
    } else if (betweenleftincl (myID, sd, p.x)) {
      // Did we get someone strictly better?
      bool ok = locations->insert (p);
      if (ok)
	oldsucc = p.x;
    } else {
      // Our successor appears to be confused, better tell
      // him what we think.
      myvnode->notify (sd, myID);
    }
  }
}

// ===== fingerlike methods ======
// XXX currently an exhaustive search of the successors
chordID
succ_list::closestpred (const chordID &x, vec<chordID> failed)
{
  chordID best = myID;
  for (u_int i = 0; i < num_succ (); i++) {
    chordID n = locations->closestsuccloc (best + 1);
    if (between (myID, x, n) && (!in_vector (failed, n)))
      best = n;
  }
  return best;
}

chordID 
succ_list::closestpred (const chordID &x)
{
  chordID best = myID;
  for (u_int i = 0; i < num_succ (); i++) {
    chordID n = locations->closestsuccloc (best + 1);
    if (between (myID, x, n))
      best = n;
  }
  return best;
}

chordID 
succ_list::closestsucc (const chordID &x) 
{
  fatal << "not implemented\n";
}


class succiter : public fingerlike_iter {
  friend class succ_list;
public:
  succiter () : fingerlike_iter () {};
};

ref<fingerlike_iter>
succ_list::get_iter ()
{
  ref<succiter> iter = New refcounted<succiter> ();

  chordID id = myID;
  u_int ns = num_succ ();
  if (ns == 0) {
    iter->nodes.push_back (id);
    return iter;
  }
  
  for (u_int i = 0; i < ns; i++) {
    id = locations->closestsuccloc (id + 1);  
    iter->nodes.push_back (id);
  }

  return iter;
}
