#include "chord.h"
#include "pred_list.h"
#include "chord_util.h"
#include "location.h"

pred_list::pred_list (ptr<vnode> v,
		      ptr<locationtable> locs,
		      chordID ID)
  : myID (ID), v_ (v), locations (locs),
    nout_continuous (0),
    gotfingers_ (false),
    nout_backoff (0),
    stable_predlist (false)
{
  backkey_ = 0;

  oldpred_ = myID;
  
  locations->pinpred (myID);
  locations->pinpredlist (myID);
}

chordID
pred_list::pred ()
{
  return locations->closestpredloc (myID);
}

vec<chord_node>
pred_list::preds ()
{
  vec<chord_node> ret;
  chordID cur = pred ();
  chordID start = cur;
  chord_node n;

  locations->get_node (cur, &n);
  ret.push_back (n);

  cur = locations->closestpredloc (decID (cur));
  for (u_int i = 1; i < NPRED && cur != start; i++) {
    locations->get_node (cur, &n);
    ret.push_back (n);
    cur = locations->closestpredloc (decID (cur));
  }
  return ret;
}
  

void
pred_list::update_pred (const chordID &p, const net_address &r)
{
  chordID curp = pred ();
  
  bool ok = true;
  if (!gotfingers_ || between (curp, myID, p))
    ok = locations->insert (p, r);
  
  if (!gotfingers_ && ok)
    v_->get_fingers (p, wrap (this, &pred_list::update_pred_fingers_cb));

  oldpred_ = pred ();
}

void
pred_list::update_pred_fingers_cb (vec<chord_node> nlist, chordstat s)
{
  if (s)
    return;
  gotfingers_ = true;
  for (unsigned i = 0; i < nlist.size (); i++)
    locations->insert (nlist[i].x, nlist[i].r);
}

void
pred_list::stabilize_pred ()
{
  chordID p = pred ();

  assert (nout_continuous == 0);

  nout_continuous++;
  v_->get_successor (p, wrap (this, &pred_list::stabilize_getsucc_cb, p));
}

void
pred_list::stabilize_getsucc_cb (chordID pred, 
				 chordID s, net_address r, chordstat status)
{
  // receive successor from my predecessor; in stable case it is me
  nout_continuous--;
  if (status) {
    warnx << myID << ": stabilize_pred: " << pred 
	  << " failure " << status << "\n";
  } else {
    // maybe we're not stable. insert this guy's successor in
    // location table; maybe he is our predecessor.
    if (!gotfingers_ || s != myID) {
      update_pred (s, r);
    }
  }
}

void
pred_list::stabilize_predlist ()
{
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
}

void
pred_list::stabilize_predlist_gotpred (chordID p, route r, chordstat stat)
{
  assert (locations->cached (p));
  v_->get_succlist (p, wrap (this, &pred_list::stabilize_predlist_gotsucclist));
}

void
pred_list::stabilize_predlist_gotsucclist (vec<chord_node> sl, chordstat s)
{
  nout_backoff--;
  stable_predlist = true;
  for (u_int i = 0; i < sl.size (); i++) {
    if (locations->cached (sl[i].x))
      continue;
    stable_predlist = false;
    warnx << myID << ": stabilize_predlist adding " << sl[i].x << "\n";
    // XXX should ping these nodes?
    bool ok = locations->insert (sl[i].x, sl[i].r);
    const char *stat = ok ? " bad " : " new ";
    warnx << myID << ": stabilize_predlist: received " << stat << "predecessor "
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
  stabilize_predlist ();
  return;
}

bool
pred_list::isstable ()
{
  // Won't be true until update_pred has been called once.
  return oldpred_ == pred () && stable_predlist;
}

void
pred_list::fill_nodelistresext (chord_nodelistextres *res)
{
  // XXX it's not always safe to go backwards. Nodes we run
  //     into going backwards might point off the ring!
  u_int i = 0;
  res->resok->nlist.setsize (NPRED); // over allocate
  chordID curpred = locations->closestsuccloc (backkey_);
  for (i = 0; (i < NPRED) && curpred != myID; i++) {
    locations->fill_getnodeext (res->resok->nlist[i], curpred);
    curpred = locations->closestsuccloc (incID (curpred));
  }
  res->resok->nlist.setsize (i + 1);
}

void
pred_list::fill_nodelistres (chord_nodelistres *res)
{
  u_int i = 0;
  res->resok->nlist.setsize (NPRED); // over allocate
  chordID curpred = locations->closestsuccloc (backkey_);
  for (i = 0; (i < NPRED) && curpred != myID; i++) {
    res->resok->nlist[i].x = curpred;
    res->resok->nlist[i].r = locations->getaddress (curpred);
    curpred = locations->closestsuccloc (incID (curpred));
  }
  res->resok->nlist.setsize (i + 1);
}
