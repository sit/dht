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

/* jy: Accordion ugliness so far
   use recargs->retry's first bit to signify primary lookup path from redundant ones.
   use recargs->succs_desired's first byte to signify the parallelism used by previous hop
  */


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
  ok = ok && set_int ("accordion.stopearly", 0);

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
  ok = Configurator::only ().get_int ("accordion.stopearly", stopearly_);
  burst_ = burst_ * budget_;

  bavail_t = timenow;
  bavail = burst_;
  me_->set_budget (budget_);

  addHandler (accordion_program_1, wrap (this,&accordion::dispatch));

  delaycb (60, wrap (this, &accordion::periodic));

  lookups_ = explored_ = 0;
  para_ = 3;
}

void
accordion::stabilize ()
{
  fingers_->start ();
  vnode_impl::stabilize ();
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

  //grab the src
  ptr<location> src;
  if (a->procno == ACCORDIONPROC_FILLGAP 
      || a->procno == ACCORDIONPROC_LOOKUP
      || a->procno == ACCORDIONPROC_LOOKUP_COMPLETE) {
    chord_node srcn;
    a->fill_from (&srcn);
    src = locations->insert (srcn);
  }

  switch (a->procno) {
  case ACCORDIONPROC_NULL:
    a->reply (NULL);
    break;
  case ACCORDIONPROC_FILLGAP:
    {
      accordion_fillgap_arg *ra = a->Xtmpl getarg<accordion_fillgap_arg> ();
      dofillgap (a,ra, src);
    }
    break;
  case ACCORDIONPROC_LOOKUP:
    {
      recroute_route_arg *ra = a->Xtmpl getarg<recroute_route_arg> ();
      doaccroute (a,ra);
    }
    break;
  case ACCORDIONPROC_LOOKUP_COMPLETE:
    {
      recroute_complete_arg *ca = 
	a->Xtmpl getarg<recroute_complete_arg> ();
      docomplete (a, ca, src);
    }
    break;
  case ACCORDIONPROC_GETFINGERS_EXT:
    {
      dogetfingers_ext (a);
      break;
    }
  default:
    {
      acctrace << (myID>>144) << ": dispatch rejected " << a->procno << "\n";
      a->reject (PROC_UNAVAIL);
      break;
    }
  }
}

/* responding to others' FILLGAP message */
void
accordion::dofillgap(user_args *sbp, accordion_fillgap_arg *ra, ptr<location> src)
{
  chord_nodelistres res (CHORD_OK);
  res.resok->nlist.setsize (1);
  my_location ()->fill_node (res.resok->nlist[0]);
  ptr<location> end = New refcounted<location> (make_chord_node (ra->end));
  vec<ptr<location> > fs = fingers_->get_fingers (src, end->id (), ra->para);
  acctrace << (myID>>144) << ": dofillgap " << " src " << (src->id ()>>144) 
    << " end " << (end->id ()>>144) << " para " << ra->para << " sz " << fs.size () << "\n";
  fill_nodelistres (&res, fs);

  sbp->reply (&res);
}

/* called by accordion_table to initiate a FILLGAP message */
void
accordion::fill_gap (ptr<location> n, 
                          ptr<location> end, cbchordIDlist_t cb)
{
  chord_nodelistres *res = New chord_nodelistres (CHORD_OK);
  ptr<accordion_fillgap_arg> fa = New refcounted<accordion_fillgap_arg> ();
  fa->para = para_;
  end->fill_node (fa->end);
  acctrace << (myID>>144) << ": fill_gap " << (my_location ()->id ()>>144) << " from " << n->id () 
    << " end " << (end->id ()>>144) << "\n";
  bytes_sent (BYTES_PER_ID);
  doRPC (n, accordion_program_1, ACCORDIONPROC_FILLGAP, fa, res,
      wrap (mkref (this), &accordion::fill_gap_cb, n, cb, res),
      wrap (mkref (this), &accordion::fill_gap_timeout_cb, n));
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
    bytes_sent (BYTES_PER_ID * res->resok->nlist.size ());
    for (unsigned i = 0; i < res->resok->nlist.size (); i++) 
      nlist.push_back (make_chord_node (res->resok->nlist[i]));
    cb (nlist, CHORD_OK);
  }
  delete res;
}

bool
accordion::fill_gap_timeout_cb (ptr<location> l,
                                chord_node n,
				int rexmit_number)
{
  if (rexmit_number == 0) {
    fingers_->del_node (l->id ());
  }
  return true;
}

void
accordion::dogetfingers_ext (user_args *sbp)
{
  chord_nodelistextres res (CHORD_OK);
  fingers_->fill_nodelistresext (&res);
  sbp->reply (&res);
}

void
accordion::periodic()
{
  //adjust the parallelism level
#define MAX_PARALLELISM 6 
  int old_p = para_;
  if (lookups_ < explored_) {
    para_++;
    if (para_ > MAX_PARALLELISM)
      para_ = MAX_PARALLELISM;
  }else if (!explored_ && lookups_) {
    para_ = para_/2;
    if (!para_) para_ = 1;
  }
  lookups_ = explored_ = 0;
  acctrace << (myID>>144) << " re-adjust parallelism from " << old_p 
    << " to " << para_ << "\n";

  //traverse the hash table that remembers
  //all routeid forwarded, delete those that are old
  sentinfo *curr = sent.first ();
  sentinfo *next;
  while (curr) {
    next = sent.next (curr);
#define ROUTEID_EXPIRE_TIME 100
    if (curr->sent_t && (timenow-curr->sent_t > ROUTEID_EXPIRE_TIME)) 
      sent.remove (curr);
    curr = next;
  }
  delaycb (ROUTEID_EXPIRE_TIME, wrap (this, &accordion::periodic));
}

void
accordion::doaccroute (user_args *sbp, recroute_route_arg *ra)
{
  chord_nodelistres res (CHORD_OK);
  ptr<location> loc;
  ptr<location> ori =  New refcounted<location> (make_chord_node (ra->origin));
  locations->insert (ori);

  if (ra->path.size () > 0) 
    loc = New refcounted<location> (make_chord_node (ra->path[ra->path.size ()-1]));
  else
    loc = ori;

  if (loc->id () != myID) {
    vec<ptr<location> > fs = fingers_->get_fingers (loc, ra->x, (ra->succs_desired >> 24));
    strbuf tmpbuf;
    for (unsigned xx = 0; xx < fs.size (); xx++) 
      tmpbuf << (fs[xx]->id ()>>144) << "," << fs[xx]->knownup () << "," << fs[xx]->age () << " ";
    acctrace << (myID>>144) << ": key " << (ra->x>>144) 
      << " give prev hop " << (loc->id ()>>144) << " para " << (ra->succs_desired >> 24) << " info " 
      << fs.size () << ": " << tmpbuf << "\n";
    fill_nodelistres (&res, fs);
  }
  ra->succs_desired &= 0x00ffffff;

  sentinfo *s = sent[ra->routeid];
  if (!s) { 
    vec<ptr<location> > cs = succs ();
    if ((stopearly_ && betweenrightincl (myID, cs.back ()->id (), ra->x)) || 
      (betweenrightincl(myID, cs.front ()->id (), ra->x))) {
      while (betweenrightincl (myID, ra->x, cs.front ()->id ()))
	cs.pop_front ();
      if ((cs.size () >= ra->succs_desired) || betweenrightincl(myID, cs.front ()->id (), ra->x)) {
	s = New sentinfo (ra->routeid, timenow, 1);
	sent.insert (s);
	lookups_++;
	doaccroute_sendcomplete (ra,cs);
      }
    }
  }

  if (s || ((ra->retries & 0x80000000) && (bavail< 0))) { //well it's a duplicate or redundant lookup
    acctrace << (myID>>144) << ": doaccroute key " << (ra->x>>144) << " seen " << (s?1:0) << " routeid " 
      << ra->routeid << " redun " << ((ra->retries & 0x80000000)?1:0) << " bavail " << bavail << "\n";
  } else {
    lookups_++;
    //calculate parallelism 
    s = New sentinfo (ra->routeid, timenow, 1);
    sent.insert (s);
    unsigned pp = get_parallelism ();
    vec<ptr<location> > nextlocs = fingers_->nexthops (ra->x, pp, s->tried);
    s->tried.setsize (nextlocs.size ());
    s->outstanding = nextlocs.size ();
    int j = 0;
    ptr<location> tmp;
    for (unsigned i = 0; i < nextlocs.size(); i++) {
      assert (nextlocs[i]->id () != myID);
      doaccroute_sendroute (ra, nextlocs[i], i, nextlocs.size ());
      s->tried[i] = nextlocs[i];
      j = i;
      while (j >0 && 
	  betweenrightincl (myID, s->tried[j-1]->id (), s->tried[j]->id ())) {
	tmp = s->tried[j-1];
	s->tried[j-1] = s->tried[j];
	s->tried[j] = tmp;
	j--;
      }
    }
    chordID xxx = nextlocs.size()?(nextlocs[0]->id ()):0;
    acctrace << (myID>>144) << ": parallel " << pp << " " << nextlocs.size () 
      << " bavail " << bavail << " key " << (ra->x>>144) << " best " << (xxx>>144) 
      << " knownup " << (nextlocs.size()?nextlocs[0]->knownup ():0) 
      << " age " <<  (nextlocs.size()?nextlocs[0]->age ():0) << "\n";
  }

  if (sbp)
    sbp->reply (&res);
  sbp = NULL;
  return;
}

void
accordion::doaccroute_sendroute (recroute_route_arg *ra, ptr<location> p, bool redun, unsigned para)
{
  chord_node_wire me;
  my_location ()->fill_node (me);

  ptr<recroute_route_arg> nra = New refcounted<recroute_route_arg> ();
  *nra = *ra;
  nra->succs_desired |= (para << 24);

  if (redun)
    nra->retries |= 0x80000000;
  nra->path.setsize (ra->path.size () + 1);
  for (unsigned i = 0; i < ra->path.size (); i++) 
    nra->path[i] = ra->path[i];
  nra->path[ra->path.size ()] = me;

  chord_nodelistres *res = New chord_nodelistres (CHORD_OK);
  long *seqnop = (long *)malloc (sizeof(long));

  bytes_sent (BYTES_PER_LOOKUP + ra->path.size ()*BYTES_PER_ID);
  long seqno = doRPC (p, accordion_program_1, ACCORDIONPROC_LOOKUP,
      nra, res,
      wrap (this, &accordion::accroute_hop_cb, seqnop, nra, p, res),
      wrap (this, &accordion::accroute_hop_timeout_cb, seqnop, getusec ()/1000, nra, p));

  *seqnop = seqno;
  acctrace << (myID>>144) << ": doaccroute_sendroute key " << (ra->x>>144) 
    << " send to " << (p->id ()>>144) << " seqno " << seqno << "\n";
}

void
accordion::accroute_hop_cb (long *seqp, ptr<recroute_route_arg> nra,
				 ptr<location> p,
				 chord_nodelistres *res,
				 clnt_stat status)
{

  sentinfo* ss = sent[nra->routeid];
  if (!status && res->status == CHORD_OK) {
    bytes_sent (res->resok->nlist.size () * BYTES_PER_ID);
    if (ss)
      ss->outstanding = 0;//i.e. no more unncessary retransmissions
    vec<chord_node> nlist;
    for (unsigned i = 0; i < res->resok->nlist.size (); i++)
      nlist.push_back (make_chord_node (res->resok->nlist[i]));
    fingers_->fill_gap_cb (p, nlist, (chordstat)0);
    
    acctrace << (myID>>144) << ": accroute_hop_cb key " << (nra->x>>144)
      << " routeid " << nra->routeid << " done forwarded to " 
      << (p->id ()>>144) << " knownup " << p->knownup () << " age " << p->age () 
      <<" learnt " << nlist.size () << " seqno " << (*seqp) << "\n";

    delete res; 
    res = NULL;
    return;
  }

  delete res;
}

bool
accordion::accroute_hop_timeout_cb (long *seqp, u_int64_t t, ptr<recroute_route_arg> nra,
				    ptr<location> p,
				    chord_node n,
				    int rexmit_number)
{
  u_int64_t now = getusec ()/1000;
  sentinfo* ss = sent[nra->routeid];
  acctrace << (myID>>144) << ": accroute_hop_TIMEOUT_cb key " << (nra->x>>144) << " next " 
    << p->id () << " seqno " << (*seqp) << " rex " << rexmit_number << "\n";
  if (rexmit_number == 0) {
    fingers_->del_node (p->id ()); 
    if (!ss || ss->outstanding == 1 || (!(nra->retries & 0x80000000))) {
      nra->retries += (now - t);
      vec<ptr<location> > nexts = fingers_->nexthops (nra->x, 1, ss->tried);
      if (nexts.size () > 0 && nexts[0]->id () != myID) {
	chord_nodelistres *nres = New chord_nodelistres (CHORD_OK);
	bytes_sent (BYTES_PER_LOOKUP + nra->path.size ()*BYTES_PER_ID);
	long *nseq = (long *)malloc (sizeof(long));
	*nseq = doRPC (nexts[0], accordion_program_1, ACCORDIONPROC_LOOKUP,
	      nra, nres,
	      wrap (this, &accordion::accroute_hop_cb, nseq , nra, nexts[0], nres),
	      wrap (this, &accordion::accroute_hop_timeout_cb, nseq , now, nra, nexts[0]));
	acctrace << (myID>>144) << ": accroute_hop_timeout_cb key " << 
	  (nra->x>>144) << " failed " << (p->id ()>>144)<< " resend next " 
	  << (nexts[0]->id ()>>144) << " seq " << (*nseq) << " rex " 
	  << rexmit_number << " routeid " << nra->routeid << " retries " 
	  <<  (nra->retries& 0x7fffffff) << " really " << (now-t) << "\n";
	return true;
      }  
      vec<ptr<location> > cs = succs ();
      chordID succid =  (cs.size ()>0?cs[0]->id ():0);
      if ((stopearly_ && betweenrightincl (myID, cs.back ()->id (), nra->x)) || 
	        (betweenrightincl(myID, cs.front ()->id (), nra->x))) {
	while (betweenrightincl (myID, nra->x, cs.front ()->id ()))
	  cs.pop_front ();
	nra->succs_desired &= 0x00ffffff;
	if (cs.size () >= nra->succs_desired) {
	  doaccroute_sendcomplete (nra,cs);
	  //make sure nra is not garbage collected
	  acctrace << (myID>>144) << ": accroute_hop_timeout_cb key " << 
	  (nra->x>>144) << " failed " << (p->id ()>>144)<< " completed rex "
	  << rexmit_number << " routeid " << nra->routeid << "\n";
	  return true;
	}
      }
      acctrace << (myID>>144) << ": accroute_hop_timeuout_cb key " 
	<< (nra->x>>144) << " succ " 
	<< (succid>>144)
	<< " SHOULD NOT HAVE HAPPEN\n";
    }
    acctrace << (myID>>144) << ": accroute_hop_timeout_cb key " 
      << (nra->x>>144) << " next " << (p->id ()>>144) << " rex " << 
      rexmit_number << " out " << ss->outstanding << " redun? " 
      << ((nra->retries&0x80000000)?1:0) << " no retry\n";
    if (ss)
      ss->outstanding--;
  } else 
      acctrace << (myID>>144) << ": accroute_hop_timeout_cb key " 
	<< (nra->x>>144)<< " next " << (p->id ()>>144) << " rex " 
	<< rexmit_number << " out " << ss->outstanding << " routeid " << 
	nra->routeid << " failed dropped\n";
  return true;
}

void
accordion::docomplete (user_args *sbp, recroute_complete_arg *ca, ptr<location> src)
{
  route_accordion *router = routers[ca->routeid];
  sentinfo* ss = sent[ca->routeid];
  if (ss)
    ss->outstanding = 0;

  if (!router) {
    acctrace << (myID>>144) << ": docomplete: unknown routeid " 
      << ca->routeid << " from host " << (src->id ()>>144) << "\n";
    sbp->reply (NULL);
    return;
  }
  strbuf s;
  s << (myID>>144)  << ": docomplete: key " << (router->key () >>144)
    << " routeid " << ca->routeid << " num_succ " <<  ca->body.robody->successors.size ()
    << " returned from " << (src->id ()>>144) << " retries: " << (ca->retries&0x7fffffff);

  chord_node n;
  for (size_t i = 0; i < ca->body.robody->successors.size (); i++) {
    n = make_chord_node (ca->body.robody->successors[i]);
    locations->insert (n);
    if (i == 0) 
      s << " succ " << (n.x>>144);
  }
  acctrace << s << "\n";
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

  chordID xxx = cs.size () > 0?cs[0]->id ():0;
  acctrace << (myID>>144) << ": doaccroute_sendcomplete key " << (ra->x>>144) 
    << " routeid " << ra->routeid << " desired " << ra->succs_desired
    << " rt " << cs.size () <<  " " << (xxx>>144) << " ori " << (l->id ()>>144) <<"\n";

  bytes_sent (BYTES_PER_LOOKUPCOMPLETE + (ra->path.size ()+ tofill)* BYTES_PER_ID);
  doRPC (l, accordion_program_1, ACCORDIONPROC_LOOKUP_COMPLETE,
	 ca, NULL,
	 wrap (this, &accordion::accroute_sent_complete_cb, l, ra->x),
	 wrap (this, &accordion::accroute_sent_complete_timeout_cb, l, ra->x));

}

void
accordion::accroute_sent_complete_cb(ptr<location> l, chordID x, clnt_stat status)
{
  if (status)
    acctrace << (myID>>144) << ": accroute_complete lost key " 
      << (x>>144) << " status " << status << ".\n";
}

bool
accordion::accroute_sent_complete_timeout_cb(ptr<location> l, chordID x, chord_node n, int rexmit_number)
{
  acctrace << (myID>>144) << ": accroute_sent_complete_timeout_cb key " << (x>>144)
    << " rexmit " << rexmit_number << "\n";
  return false;
}

void
accordion::fill_nodelistres (chord_nodelistres *res, vec<ptr<location> > fs)
{
  res->resok->nlist.setsize (fs.size ());
  for (size_t i = 0; i < fs.size (); i++)
    fs[i]->fill_node (res->resok->nlist[i]);
}


void
accordion::update_bavail ()
{
  if (timenow > 0 && bavail_t > 0)
    bavail += (budget_ * (timenow-bavail_t));
  bavail_t = timenow;
  if (bavail > burst_) 
    bavail = burst_;
}

unsigned 
accordion::get_parallelism ()
{
  update_bavail();
  int p = bavail/(BYTES_PER_LOOKUP+BYTES_PER_ID);
  if (p <= 0)
    return 1;
  else 
    return para_;
}

