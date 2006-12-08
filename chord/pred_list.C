// This implementation is just set up to maintain strict predecessors.
// Some code exists to find predecessor lists but it will give wrong answers.
// See pred_list.h.

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
#ifdef PRED_LIST
    //    backkey_ (0),
    npred_ (0),
#endif /* PRED_LIST */    
    nout_continuous (0),
    nout_backoff (0),
    stable_predlist (false)
{
  oldpred_ = v->my_location ();
  
  locations->pin (myID, -1);
#ifdef PRED_LIST  
  bool ok = Configurator::only ().get_int ("chord.nsucc", npred_);
  assert (ok);
  locations->pin (myID, -npred_);
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
  for (u_int i = 1; i < num_pred (); i++) {
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
  if (between (curp->id (), myID, p.x))
    ploc = locations->insert (p);

  oldpred_ = pred ();
}

//XXX this doesn't appear in the PODC paper which relies only on notify to 
//    stabilize the pred. Jinyang suggests that it helped performance under
//    the simultator, however.

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
    if (s.x != myID)
      update_pred (s);
  }
}

void
pred_list::stabilize_predlist ()
{
#ifdef PRED_LIST  
  //get pred's pred list
  ptr<location> p = pred ();
  if (p) {
    v_->get_predlist (p, wrap (this, &pred_list::stabilize_predlist_cb));
    nout_backoff++;
  }
#endif /* PRED_LIST */  
}

void
pred_list::stabilize_predlist_cb (vec<chord_node> pl,
				  chordstat stat)
{
  nout_backoff--;
  
  // Wait until next time to stabilize me.
  if (stat)
    return;
  
  stable_predlist = true;
  for (u_int i = 0; i < pl.size (); i++) {
    if (locations->cached (pl[i].x))
      continue;
    stable_predlist = false;
    // XXX should ping this nodes to ensure they are up?
    bool ok = locations->insert (pl[i]);
    const char *stat = ok ? "new" : "bad";
    warnx << myID << ": stabilize_predlist: received " 
	  << stat << " predecessor "
	  << pl[i] << ".\n";
    
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
#ifdef PRED_LIST
  // XXX it's not always safe to go backwards. Nodes we run
  //     into going backwards might point off the ring!
  u_int i = 0;
  unsigned int curnpred = num_pred ();
  res->resok->nlist.setsize (curnpred + 1); // over allocate
  ptr<location> curpred = v_->my_location ();
  for (i = 0; (i <= curnpred); i++) {
    curpred->fill_node_ext (res->resok->nlist[i]);
    curpred = locations->closestpredloc (decID (curpred->id ()));
  }
#else
  fatal << "not implemented.\n";
#endif /* 0 */
}

unsigned int
pred_list::num_pred ()
{
  int goodnodes = locations->usablenodes () - 1;
  int newnpred = (npred_ > goodnodes) ? goodnodes : npred_;
  
  if (newnpred < 0) {
    warn << "pred_list::num_pred () n:" << newnpred 
	 << " g:" << goodnodes << "\n";
    newnpred = 0;
  }
  return newnpred;
}

void
pred_list::fill_nodelistres (chord_nodelistres *res)
{
#ifdef PRED_LIST  
  u_int i = 0;
  unsigned int curnpred = num_pred ();
  res->resok->nlist.setsize (curnpred + 1); // over allocate
  ptr<location> curpred = v_->my_location ();
  for (i = 0; (i <= curnpred); i++) {
    curpred->fill_node (res->resok->nlist[i]);
    curpred = locations->closestpredloc (decID (curpred->id ()));
  }
#else
  fatal << "not implemented.\n";
#endif /* PRED_LIST */
}
