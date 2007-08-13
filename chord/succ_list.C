#include "chord.h"
#include "succ_list.h"
#include <location.h>
#include <locationtable.h>
#include <id_utils.h>
#include <misc_utils.h>

#include <configurator.h>

/*
 * The idea of the succ_list class is to maintain a list of
 * immediate successors to the current node. The actual number
 * of successors needed is up to nsucc. The location information
 * is maintained in locationtable.
 */
succ_list::succ_list (ptr<vnode> v,
		      ptr<locationtable> locs)
  : myID (v->my_ID ()), myvnode (v), locations (locs)
{
  nnodes = 0;
  
  bool ok = Configurator::only ().get_int ("chord.nsucc", nsucc_);
  assert (ok);
  
  oldsucc = v->my_location ();
  stable_succlist = false;
  stable_succlist2 = false;
  nout_backoff = 0;
  nout_continuous = 0;

  locations->pin (myID, 1);
  locations->pin (myID, nsucc_);
}
   
ptr<location>
succ_list::succ ()
{
  return locations->closestsuccloc (incID (myID));
}

vec<ptr<location> >
succ_list::succs ()
{
  vec<ptr<location> > ret;
  
  ptr<location> cur = succ ();
  ret.push_back (cur);

  for (u_int i = 1; i < num_succ (); i++) {
    cur = locations->closestsuccloc (incID (cur->id ()));
    ret.push_back (cur);
  }
  return ret;
}

unsigned int
succ_list::num_succ ()
{
  int goodnodes = locations->usablenodes ();
  int newnsucc = (nsucc_ > goodnodes) ? goodnodes : nsucc_;
  
  if (newnsucc < 0) {
    warn << "succ_list::num_succ () n:" << newnsucc << " g:" << goodnodes << "\n";
    newnsucc = 0;
  }
  return newnsucc;
}

void
succ_list::print (strbuf &outbuf)
{
  vec<ptr<location> > s = succs ();
  for (u_int i = 0; i < s.size (); i++)
    outbuf << myID << ": succ " << i + 1 << " : " << s[i]->id () << "\n";
}

u_long
succ_list::estimate_nnodes ()
{
  u_long n;
  ptr<location> lastsucc = myvnode->my_location ();
  int nsucc = num_succ ();
  for (int i = 0; i < nsucc; i++)
    lastsucc = locations->closestsuccloc (incID (lastsucc->id ()));
  
  chordID d = diff (myID, lastsucc->id ());
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
  ptr<location> cursucc = myvnode->my_location ();
  res->resok->nlist.setsize (curnsucc + 1);
  for (int i = 0; i <= curnsucc; i++) {
    cursucc->fill_node_ext (res->resok->nlist[i]);
    cursucc = locations->closestsuccloc (incID (cursucc->id ()));
  }
}

void
succ_list::fill_nodelistres (chord_nodelistres *res)
{
  // succ[0] is me. the rest are our actual successors.
  int curnsucc = num_succ ();
  ptr<location> cursucc = myvnode->my_location ();
  res->resok->nlist.setsize (curnsucc + 1);
  for (int i = 0; i <= curnsucc; i++) {
    cursucc->fill_node (res->resok->nlist[i]);
    cursucc = locations->closestsuccloc (incID (cursucc->id ()));
  }
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
  ptr<location> s = succ ();
  myvnode->get_succlist
    (s, wrap (this, &succ_list::stabilize_getsucclist_cb, s));
}

void
succ_list::stabilize_getsucclist_cb (ptr<location> s, vec<chord_node> nlist,
				     chordstat status)
{
  nout_backoff--;
  if (status) {
    warnx << myID << ": stabilize_getsucclist_cb: " << s << " : " 
	  << "failure " << status << "\n";
    stable_succlist = false;
    return;
  }
  assert (s->id () == nlist[0].x);

  // PODC paper says, set our succlist to successors succlist mod last guy
  // We're going to merge his succlist with our succlist and make sure
  // deletions and insertions are accurate from our POV.
  unsigned int i, j;
  vec<ptr<location> > succlist = succs ();
  size_t curnsucc = succlist.size ();

  i = 0; j = 0;
  unsigned int newnsucc = nlist.size () - 1; // drop last guy.
  while ((i < curnsucc) && (j < newnsucc)) {    
    if (succlist[i]->id () == nlist[j].x) {
      succlist[i]->update (nlist[j]);
      i++; j++; continue;
    }
    if (between (myID, nlist[j].x, succlist[i]->id ())) {
      // if succlist[i] < nlist[j].x
      // then, maybe someone we knew about is dead now. best be sure.
      nout_backoff++;
      myvnode->ping
	(succlist[i], wrap (this, &succ_list::stabilize_getsucclist_check,
			    s, succlist[i]->id ()));
      i++;
      continue;
    }
    if (between (myID, succlist[i]->id (), nlist[j].x)) {
      // if succlist[i] > nlist[j].x
      // then maybe a new node joined. check it out.
      ptr<location> newsucc = locations->insert (nlist[j]);
      if (!newsucc || !newsucc->alive ()) {
	warnx << myID << ": stabilize_succlist: received bad successor "
	      << nlist[j].x << " from " << s << "\n";
	// XXX do something about it?
      } else {
	stable_succlist = false;
	warnx << myID << ": stabilize_succlist: received new successor "
	      << nlist[j].x << "," << nlist[j].knownup << "," 
	      << nlist[j].age << " from " << (s->id ()>>144) << "," << s->knownup () 
	      << "," << s->age () << "\n";
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
			  s, succlist[i]->id ()));
    i++;
  }

  while (j < newnsucc && curnsucc < (u_int)nsucc_) {
    assert (!check);
    ptr<location> newsucc = locations->insert (nlist[j]);
    if (!newsucc || !newsucc->alive ()) {
      warnx << myID << ": stabilize_succlist: received bad successor "
	    << nlist[j].x << " from " << s << "\n";
      // XXX do something about it?
    } else {
      curnsucc++;
      stable_succlist = false;
      warnx << myID << ": stabilize_succlist (2): received new successor "
	    << nlist[j].x << " from " << s << "\n";
    }
    j++;
  }
}

void
succ_list::stabilize_getsucclist_check (ptr<location> src, chordID chk,
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
  ptr<location> cursucc = succ ();
  if (cursucc != oldsucc) {
    warnx << myID << ": my successor changed from "
	  << oldsucc->id () << " to " << cursucc->id () << "\n";
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
succ_list::stabilize_getpred_cb (ptr<location> sd, chord_node p, chordstat status)
{
  nout_continuous--;
  // receive predecessor from my successor; in stable case it is me
  if (status) {
    warnx << myID << ": stabilize_getpred_cb " << sd->id ()
    	  << " failure status " << status << "\n";
    if (status == CHORD_ERRNOENT) {
      warnx << myID << ": stabilize_getpred_cb " << sd->id ()
	    << " doesn't know about his predecessor?? notifying...\n";
      myvnode->notify (sd, myID);
    }
    // other failures we will address next round.
  } else {
    if (myID == p.x) {
      // Good, things are as we expect.
    } else if (betweenleftincl (myID, sd->id (), p.x)) {
      // Did we get someone strictly better?
      ptr<location> newsucc = locations->insert (p);
      if (newsucc && newsucc->alive ())
	oldsucc = newsucc;
    } else {
      // Our successor appears to be confused, better tell
      // him what we think.
      myvnode->notify (sd, myID);
    }
  }
}

// XXX currently an exhaustive search of the successors
ptr<location>
succ_list::closestpred (const chordID &x, vec<chordID> failed)
{
  ptr<location> best = myvnode->my_location ();
  for (u_int i = 0; i < num_succ (); i++) {
    ptr<location> n = locations->closestsuccloc (incID (best->id ()));
    if (between (myID, x, n->id ()) && (!in_vector (failed, n->id ())))
      best = n;
  }
  return best;
}

