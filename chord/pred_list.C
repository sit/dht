// Predecessor lists are totally broken. DO NOT USE!
// This implementation is just set up to maintain strict predecessors.
#undef PRED_LIST

#include "chord.h"
#include "pred_list.h"

#ifdef PRED_LIST
#include <configurator.h>
#endif /* PRED_LIST */
#include <id_utils.h>
#include <location.h>
#include <locationtable.h>
#include <misc_utils.h>

pred_list::pred_list (ptr<vnode> v,
		      ptr<locationtable> locs)
  : myID (v->my_ID ()), v_ (v), locations (locs),
    nout_continuous (0),
    gotfingers_ (false),
    nout_backoff (0),
    stable_predlist (false)
{
  backkey_ = 0;

  oldpred_ = v->my_location ();
  
  locations->pin (myID, -1);
#ifdef PRED_LIST  
  size_t npred;
  bool ok = Configurator::only ().get_int ("chord.nsucc", npred);
  assert (ok);
  locations->pinpredlist (myID, npred);
#endif /* PRED_LIST */  
}

ptr<location>
pred_list::pred ()
{
  return locations->closestpredloc (myID);
}

vec<ptr<location> >
pred_list::preds ()
{
  vec<ptr<location> > ret;
  
  ptr<location> cur = pred ();
  ret.push_back (cur);
#ifdef PRED_LIST
  ptr<location> start = cur;
  // XXX it's not always safe to go backwards. Nodes we run
  //     into going backwards might point off the ring!
  for (u_int i = 1; i < NPRED && cur != start; i++) {
    cur = locations->closestpredloc (decID (cur->id ()));
    ret.push_back (cur);
  }
#endif /* PRED_LIST */
  return ret;
}
  

void
pred_list::update_pred (const chord_node &p)
{
  ptr<location> curp = pred ();
  
  ptr<location> ploc;
  if (!gotfingers_ || between (curp->id (), myID, p.x))
    ploc = locations->insert (p);
  
  if (!gotfingers_ && ploc)
    v_->get_fingers (ploc, wrap (this, &pred_list::update_pred_fingers_cb));

  oldpred_ = pred ();
}

void
pred_list::update_pred_fingers_cb (vec<chord_node> nlist, chordstat s)
{
  if (s)
    return;
  gotfingers_ = true;
  for (unsigned i = 0; i < nlist.size (); i++)
    locations->insert (nlist[i]);
}

void
pred_list::stabilize_pred ()
{
  ptr<location> p = pred ();

  assert (nout_continuous == 0);

  nout_continuous++;
  v_->get_successor (p, wrap (this, &pred_list::stabilize_getsucc_cb, p->id ()));
}

void
pred_list::stabilize_getsucc_cb (chordID pred, chord_node s, chordstat status)
{
  // receive successor from my predecessor; in stable case it is me
  nout_continuous--;
  if (status) {
    warnx << myID << ": stabilize_pred: " << pred 
	  << " failure " << status << "\n";
  } else {
    // maybe we're not stable. insert this guy's successor in
    // location table; maybe he is our predecessor.
    if (!gotfingers_ || s.x != myID) {
      update_pred (s);
    }
  }
}

void
pred_list::stabilize_predlist ()
{
#ifdef PRED_LIST  
  u_long n = locations->usablenodes ();
  chordID preddist (1);
  // XXX should this depend on NPRED?
  preddist = (preddist << NBIT) * log2 (n) / n;
  if (preddist == 0) {
    stable_predlist = true;
    return;
  }
  // warnx << myID << ": stabilizing pred list with preddist " << preddist
  //	<< " for estimated " << n << " nodes.\n";

  backkey_ = diff (preddist, myID);
  // warnx << myID << ": searching for successor to " << backkey_ << ".\n";

  nout_backoff++;
  v_->find_successor (backkey_,
		      wrap (this, &pred_list::stabilize_predlist_gotpred));
#endif /* PRED_LIST */  
}

void
pred_list::stabilize_predlist_gotpred (vec<chord_node> sl,
				       route r, chordstat stat)
{
  nout_backoff--;

  // Wait until next time to stabilize me.
  if (stat)
    return;
  
  stable_predlist = true;
  for (u_int i = 0; i < sl.size (); i++) {
    if (locations->cached (sl[i].x))
      continue;
    stable_predlist = false;
    // XXX should ping this nodes to ensure they are up?
    bool ok = locations->insert (sl[i]);
    const char *stat = ok ? "new" : "bad";
    warnx << myID << ": stabilize_predlist: received " << stat << " predecessor "
	  << sl[i] << ".\n";

  }
}


void
pred_list::do_continuous ()
{
  stabilize_pred ();
}

void
pred_list::do_backoff ()
{
#ifdef PRED_LIST  
  stabilize_predlist ();
#endif /* PRED_LIST */  
  return;
}

bool
pred_list::isstable ()
{
  // Won't be true until update_pred has been called once.
#ifndef PRED_LIST  
  return oldpred_ == pred ();
#else /* !PRED_LIST */  
  return oldpred_ == pred () && stable_predlist;
#endif /* PRED_LIST */  
}

void
pred_list::fill_nodelistresext (chord_nodelistextres *res)
{
  fatal << "not implemented.\n";
#if 0  
  // XXX it's not always safe to go backwards. Nodes we run
  //     into going backwards might point off the ring!
  u_int i = 0;
  res->resok->nlist.setsize (NPRED); // over allocate
  chordID curpred = locations->closestsuccloc (backkey_);
  for (i = 0; (i < NPRED) && curpred != myID; i++) {
    locations->lookup (curpred)->fill_node_ext (res->resok->nlist[i]);
    curpred = locations->closestsuccloc (incID (curpred));
  }
  res->resok->nlist.setsize (i + 1);
#endif /* 0 */
}

void
pred_list::fill_nodelistres (chord_nodelistres *res)
{
  fatal << "not implemented.\n";
#if 0  
  u_int i = 0;
  res->resok->nlist.setsize (NPRED); // over allocate
  chordID curpred = locations->closestsuccloc (backkey_);
  for (i = 0; (i < NPRED) && curpred != myID; i++) {
    locations->lookup (curpred)->fill_node (res->resok->nlist[i]);
    curpred = locations->closestsuccloc (incID (curpred));
  }
  res->resok->nlist.setsize (i + 1);
#endif /* 0 */  
}
