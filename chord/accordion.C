#include "accordion.h"
#include <id_utils.h>
#include <misc_utils.h>
#include "accordion_table.h"
#include "pred_list.h"
#include "succ_list.h"

#include <location.h>
#include <locationtable.h>
#include <modlogger.h>

#include <configurator.h>

#define acctrace modlogger ("accordion")

#define MAX_RETRIES 5

static struct accordion_config_init {
  accordion_config_init ();
}aci;

accordion_config_init::accordion_config_init ()
{
  bool ok = true;

#define set_int Configurator::only ().set_int
#define set_str Configurator::only ().set_str
  ok = ok && set_int ("accordion.budget", 10);
  ok = ok && set_int ("accordion.burst", 100);

  assert (ok);
#undef set_int
#undef set_str
}

ref<vnode>
accordion::produce_vnode (ref<chord> _chordnode,
                            ref<rpc_manager> _rpcm,
			    ref<location> _l)
{
  return New refcounted<accordion> (_chordnode,_rpcm,_l);
}

// override produce_iterator*
ptr<route_iterator>
accordion::produce_iterator (chordID xi) 
{
  ptr<route_accordion> ri = New refcounted<route_accordion> (mkref (this), xi);
  routers.insert (ri);
  return ri;
}

ptr<route_iterator>
accordion::produce_iterator (chordID xi,
			       const rpc_program &uc_prog,
			       int uc_procno,
			       ptr<void> uc_args) 
{
  ptr<route_accordion> ri = New refcounted<route_accordion> (mkref (this),
							   xi, uc_prog,
							   uc_procno, uc_args);
  routers.insert (ri);
  return ri;
}

route_iterator *
accordion::produce_iterator_ptr (chordID xi) 
{
  route_accordion *ri = New route_accordion (mkref (this), xi);
  routers.insert (ri);
  return ri;
}

route_iterator *
accordion::produce_iterator_ptr (chordID xi,
				   const rpc_program &uc_prog,
				   int uc_procno,
				   ptr<void> uc_args) 
{
  route_accordion *ri = New route_accordion (mkref (this), xi, uc_prog,
					   uc_procno, uc_args);
  routers.insert (ri);
  return ri;
}

void
accordion::init ()
{
  bool ok = Configurator::only ().get_int ("accordion.budget", budget_);
  assert (ok);
  ok = Configurator::only ().get_int ("accordion.burst", burst_);
  assert (ok);
  burst_ = burst_ * budget_;

  bavail_t = getusec ()/1000000;
  bavail = burst_;
  me_->set_budget (budget_);

  stabilizer->register_client (fingers_);
  addHandler (accordion_program_1, wrap (this,&accordion::dispatch));

  //periodically clean the hash table that remembers all
  //routeids that have been forwarded so far
  delaycb (60, wrap (this, &accordion::clean_sent));

  starttime = getusec () / 1000000;
}

accordion::accordion (ref<chord> _chord,
                               ref<rpc_manager> _rpcm,
			       ref<location> _l)
  : vnode_impl(_chord,_rpcm,_l)
{
  fingers_ = New refcounted<accordion_table> (mkref(this), locations);
  init ();
}


void
accordion::dispatch (user_args *a)
{
  if (a->prog->progno != accordion_program_1.progno) {
    vnode_impl::dispatch (a);
    return;
  }
  switch (a->procno) {
  case ACCORDIONPROC_NULL:
    a->reply (NULL);
    break;
  case ACCORDIONPROC_FILLGAP:
    {
      accordion_fillgap_arg *ra = a->template getarg<accordion_fillgap_arg> ();
      dofillgap (a,ra);
    }
    break;
  case ACCORDIONPROC_LOOKUP:
    {
      recroute_route_arg *ra = a->template getarg<recroute_route_arg> ();
      doaccroute (a,ra);
    }
    break;
  case ACCORDIONPROC_LOOKUP_COMPLETE:
    {
      recroute_complete_arg *ca = 
	a->template getarg<recroute_complete_arg> ();
      docomplete (a, ca);
    }
    break;
  case ACCORDIONPROC_GETFINGERS_EXT:
    {
      dogetfingers_ext (a);
      break;
    }
  default:
    {
      acctrace << myID << " dispatch rejected " << a->procno << "\n";
      a->reject (PROC_UNAVAIL);
      break;
    }
  }
}

/* responding to others' FILLGAP message */
void
accordion::dofillgap(user_args *sbp, accordion_fillgap_arg *ra)
{
  chord_nodelistres res (CHORD_OK);
  ptr<location> src = New refcounted<location> (make_chord_node (ra->src));
  locations->insert (src);

  res.resok->nlist.setsize (1);
  my_location ()->fill_node (res.resok->nlist[0]);
  fingers_->fill_gap_nodelistres (&res, src, ra->end);

  sbp->reply (&res);
}

/* called by accordion_table to initiate a FILLGAP message */
void
accordion::fill_gap (ptr<location> n, 
                          chordID end, cbchordIDlist_t cb)
{
  chord_nodelistres *res = New chord_nodelistres (CHORD_OK);
  ptr<accordion_fillgap_arg> fa = New refcounted<accordion_fillgap_arg> ();

  my_location ()->fill_node (fa->src);

  fa->end = end;
  acctrace << "fill_gap " << my_location ()->id () << " from " << n->id () 
    << " end " << fa->end << "\n";
  doRPC (n, accordion_program_1, ACCORDIONPROC_FILLGAP, fa, res,
      wrap (mkref (this), &accordion::fill_gap_cb, n, cb, res));
}

void
accordion::fill_gap_cb (ptr<location> l, 
			     cbchordIDlist_t cb,
			     chord_nodelistres *res, 
			     clnt_stat err)
{
  vec<chord_node> nlist;
  chord_node n;

  if ((err) || (res->status)) {
    l->fill_node (n);
    nlist.push_back (n);
  }

  if (err) 
    cb (nlist, CHORD_RPCFAILURE);
  else if (res->status) 
    cb (nlist, res->status);
  else {
    for (unsigned i = 0; i < res->resok->nlist.size (); i++) 
      nlist.push_back (make_chord_node (res->resok->nlist[i]));
    cb (nlist, CHORD_OK);
  }
  delete res;
}

void
accordion::dogetfingers_ext (user_args *sbp)
{
  chord_nodelistextres res (CHORD_OK);
  fingers_->fill_nodelistresext (&res);
  sbp->reply (&res);
}

//traverse the hash table that remembers
//all routeid forwarded, delete those that are old
void
accordion::clean_sent ()
{
  time_t now = getusec ()/1000000;

  sentinfo *curr = sent.first ();
  sentinfo *next;
  while (curr) {
    next = sent.next (curr);
#define ROUTEID_EXPIRE_TIME 20
    if (now-curr->sent_t > ROUTEID_EXPIRE_TIME) 
      sent.remove (curr);
    curr = next;
  }
  delaycb (60, wrap (this, &accordion::clean_sent));
}

void
accordion::doaccroute (user_args *sbp, recroute_route_arg *ra)
{
  chord_nodelistres res (CHORD_OK);
  ptr<location> loc;

  if (ra->path.size () > 0) 
    loc = New refcounted<location> (make_chord_node (ra->path[ra->path.size ()-1]));
  else
    loc = New refcounted<location> (make_chord_node (ra->origin));

  if (loc->id () != myID)
    fingers_->fill_gap_nodelistres (&res, loc, ra->x);

  vec<ptr<location> > cs = succs ();
  u_long m = ra->succs_desired;

  acctrace << myID << ": doaccroute key " << ra->x 
    << " next (" << ra->routeid << "," << ra->x << "): " 
    << "starting; desired = " << ra->succs_desired << " backsucc " << 
    cs.back ()->id () << "\n";

  sentinfo *s = sent[ra->routeid];
  if (!s) { 
    sentinfo* ss = New sentinfo (ra->routeid, getusec ()/1000000, 1);
    sent.insert (ss);
    if (betweenrightincl (myID, cs.back ()->id (), ra->x)) {
      while (betweenrightincl (myID, ra->x, cs.front ()->id ()))
	cs.pop_front ();
      acctrace << myID << ": doaccroute key " << ra->x 
	<< " next " << cs.size() << "\n";
      if (cs.size () >= m) {
	bytes_sent (40);
	doaccroute_sendcomplete (ra,cs);
	if (sbp)
	  sbp->reply (&res);
	sbp = NULL;
	return;
      }
    }

    //calculate parallelism
    unsigned pp = get_parallelism();
    vec<ptr<location> > nextlocs = fingers_->nexthops (ra->x, pp);
    unsigned p = 0;
    for (unsigned i = 0; i < nextlocs.size(); i++) {
      if (nextlocs[i]->id () != myID) {
	bytes_sent (40);
	doaccroute_sendroute (ra, nextlocs[i]);
	p++;
      } else
	acctrace << myID << " next hop is me! " << i << " dropping\n";
    }
    acctrace << myID << " parallel lookup " << pp << " " << p << " bavail " << bavail << " for id " << ra->x << "\n";
    ss->outstanding = p;
  }

  if (sbp)
    sbp->reply (&res);
  sbp = NULL;
  return;
}

void
accordion::doaccroute_sendroute (recroute_route_arg *ra, ptr<location> p)
{
  chord_node_wire me;
  my_location ()->fill_node (me);

  ptr<recroute_route_arg> nra = New refcounted<recroute_route_arg> ();
  *nra = *ra;
  nra->path.setsize (ra->path.size () + 1);
  for (unsigned i = 0; i < ra->path.size (); i++) 
    nra->path[i] = ra->path[i];
  nra->path[ra->path.size ()] = me;

  chord_nodelistres *res = New chord_nodelistres (CHORD_OK);
  doRPC (p, accordion_program_1, ACCORDIONPROC_LOOKUP,
      nra, res,
      wrap (this, &accordion::accroute_hop_cb, nra, p, res),
      wrap (this, &accordion::accroute_hop_timeout_cb, nra, p));
}

void
accordion::accroute_hop_cb (ptr<recroute_route_arg> nra,
				 ptr<location> p,
				 chord_nodelistres *res,
				 clnt_stat status)
{

  sentinfo* ss = sent[nra->routeid];
  assert (ss->outstanding > 0);
  ss->outstanding--;

  if (!status && res->status == CHORD_OK) {

    vec<chord_node> nlist;
    for (unsigned i = 0; i < res->resok->nlist.size (); i++)
      nlist.push_back (make_chord_node (res->resok->nlist[i]));
    fingers_->fill_gap_cb (p, nlist, (chordstat)0);
    
    acctrace << myID << ": accroute_hop_cb (" << nra->routeid << ", "
      << nra->x << "): done forwarded to " << p->id () << "\n";

    delete res; 
    res = NULL;
    return;
  }

  delete res;
  nra->retries++;

  fingers_->del_node (p->id ()); //jy: should just record loss
  //XXX sth. fishy here, what if my successor fails??
  if ((p->id () != myID) && (nra->retries < MAX_RETRIES) && 
      ss->outstanding == 1) {
    vec<ptr<location> > nexts = fingers_->nexthops (nra->x, 1);
    if (nexts.size () > 0) {
      bytes_sent (40);
      chord_nodelistres *nres = New chord_nodelistres (CHORD_OK);
      doRPC (nexts[0], accordion_program_1, ACCORDIONPROC_LOOKUP,
	  nra, nres,
	  wrap (this, &accordion::accroute_hop_cb, nra, p, nres),
	  wrap (this, &accordion::accroute_hop_timeout_cb, nra, p));
      acctrace << myID << ": accroute_hop_cb (" << nra->routeid << ", "
	<< nra->x << " failed " << p->id () 
	<< " resend to " << nexts[0]->id () << "\n";
      ss->outstanding++;
    } 
  }else {
    acctrace << myID << ": accroute_hop_cb (" << nra->routeid << ", "
      << nra->x << ") dropped\n";
    ss->outstanding--;
  }
}

void
accordion::accroute_hop_timeout_cb ( ptr<recroute_route_arg> nra,
				    ptr<location> p,
				    chord_node n,
				    int rexmit_number)
{
  //jy: to be fixed. this might result duplicate sending of the same mesg
  acctrace << myID << ":accroute_hop_timeout_cb " << nra->routeid << " rexmit " << rexmit_number << "\n";
  /*
  if (rexmit_number == 1) {
    nra->retries++;
    fingers_->del_node (p->id ()); //jy: should just record loss
    fingers_->nexthops (nra->x, 1);
    vec<ptr<location> > nexts = fingers_->nexthops (nra->x, 1);
    if (nexts.size () > 0 && nexts[0]->id () != myID) {
      sentinfo* ss = sent[nra->routeid];
      assert (ss->outstanding > 0);
      ss->outstanding++;
      chord_nodelistres *nres = New chord_nodelistres (CHORD_OK);
      doRPC (nexts[0], accordion_program_1, ACCORDIONPROC_LOOKUP,
	    nra, nres,
	    wrap (this, &accordion::accroute_hop_cb, nra, p, nres),
	    wrap (this, &accordion::accroute_hop_timeout_cb, nra, p));
      acctrace << myID << ": accroute_hop_timeout_cb (" << nra->routeid << ", "
	<< nra->x << " failed " << p->id () 
	<< " resend to " << nexts[0]->id () << "\n";
     return;
    }
  }
  acctrace << myID << ":accroute_hop_timeout_cb (" << nra->routeid << ", "
    << nra->x << " failed " << p->id () << " dropped" << "\n";
    */
}

void
accordion::docomplete (user_args *sbp, recroute_complete_arg *ca)
{
  acctrace << my_ID () << ": docomplete: routeid " << ca->routeid
    << " has returned! retries: " << ca->retries << "\n";
  route_accordion *router = routers[ca->routeid];
  if (!router) {
    chord_node src; sbp->fill_from (&src);
    acctrace << my_ID () << ": docomplete: unknown routeid " << ca->routeid
		     << " from host " << src << "\n";
    sbp->reply (NULL);
    return;
  }
  
  routers.remove (router);
  router->handle_complete (sbp, ca);
}

void
accordion::doaccroute_sendcomplete ( recroute_route_arg *ra,
				    const vec<ptr<location> > cs)
{
  //this has a lot of copy-n-paste from recroute.C
  chord_node_wire me;
  my_location ()->fill_node (me);

  acctrace << myID << ": doaccroute_sendcomplete (" << ra->routeid << "," 
    << ra->x << "): complete\n";


  ptr<recroute_complete_arg> ca = New refcounted<recroute_complete_arg> ();
  ca->body.set_status ((recroute_complete_stat)CHORD_OK);
  ca->routeid = ra->routeid;
  
  ca->path.setsize (ra->path.size () + 1);
  for (size_t i = 0; i < ra->path.size (); i++) {
    ca->path[i] = ra->path[i];
  }
  ca->path[ra->path.size ()] = me;

  u_long m = ra->succs_desired;
  u_long tofill = (cs.size () < m) ? cs.size () : m;
  ca->body.robody->successors.setsize (tofill);
  for (size_t i = 0; i < tofill; i++)
    cs[i]->fill_node (ca->body.robody->successors[i]);

  ca->retries = ra->retries;
  
  ptr<location> l = locations->insert(make_chord_node (ra->origin));
  doRPC (l, accordion_program_1, ACCORDIONPROC_LOOKUP_COMPLETE,
	 ca, NULL,
	 wrap (this, &accordion::accroute_sent_complete_cb));
}

void
accordion::accroute_sent_complete_cb(clnt_stat status)
{
  if (status)
    acctrace << myID << ": accroute_complete lost, status " << status << ".\n";
}

void
accordion::update_bavail ()
{
  time_t now = getusec () / 1000000;
  bavail += (budget_ * (now-bavail_t));
  bavail_t = now;
  if (bavail > burst_) 
    bavail = burst_;
}

#define MAX_PARALLELISM 6 
unsigned 
accordion::get_parallelism ()
{
  update_bavail();
  int p = bavail/40;
  if (p <= 0)
    return 1;
  else if (p > MAX_PARALLELISM)
    return MAX_PARALLELISM;
  else
    return (unsigned) p;
}


