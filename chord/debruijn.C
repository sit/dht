#include "chord.h"

#include "debruijn.h"
#include "succ_list.h"
#include <location.h>
#include <locationtable.h>

#include <configurator.h>

struct debruijn_init {
  debruijn_init ();
} di;

debruijn_init::debruijn_init ()
{
  Configurator::only ().set_int ("debruijn.logbase", 1);
}

ref<vnode>
debruijn::produce_vnode (ref<chord> _chordnode,
			 ref<rpc_manager> _rpcm,
			 ref<location> _l)
{
  int configlogbase = 1;
  Configurator::only ().get_int ("debruijn.logbase", configlogbase);
  return New refcounted<debruijn> (_chordnode, _rpcm,
				   _l,
				   configlogbase);
}

debruijn::debruijn (ref<chord> _chord,
		    ref<rpc_manager> _rpcm,
		    ref<location> _l,
		    int l) : vnode_impl (_chord, _rpcm, _l),
			     logbase_ (l)
{
  stabilizer->register_client (mkref (this));
  mydoubleID = doubleID (my_ID (), logbase_);
  locations->pin (mydoubleID, -1); // XXX ping logbase successors of mydoubleID
  addHandler (debruijn_program_1, wrap (this, &debruijn::dispatch));
  warn << my_ID () << " de bruijn: double :" << mydoubleID << "\n";  
}

debruijn::~debruijn () {}


ptr<location>
debruijn::debruijnptr ()
{
  return locations->closestpredloc (mydoubleID);
}

ptr<location>
debruijn::closestsucc (const chordID &x)
{
  ptr<location> s = my_location ();
  ptr<location> succ = locations->closestsuccloc (myID + 1);
  ptr<location> n;

  if (betweenrightincl (myID, succ->id (), x)) s = succ;

  for (int i = 1; i < logbase_; i++) {
    n = locations->closestsuccloc (succ->id () + 1);
    if (betweenrightincl (myID, n->id (), x) && between (x, s->id (), n->id ())) {
      s = n;
    }
    succ = n;
  }

  // use closestpred, because we pinned pred
  n = locations->closestpredloc (mydoubleID); 
  if (betweenrightincl (myID, n->id (), x) && between (x, s->id (), n->id ())) {
    s = n;
  }

  return s;
}

//XXX ignores failed node list.
ptr<location>
debruijn::closestpred (const chordID &x, const vec<chordID> &f)
{
  ptr<location> succ = locations->closestsuccloc (myID + 1);
  ptr<location> p;
  ptr<location> n;

  if (betweenrightincl (myID, succ->id (), x)) p = my_location ();
  else p = succ;

  for (int i = 1; i < logbase_; i++) {
    n = locations->closestsuccloc (succ->id () + 1);
    if (between (myID, x, n->id ()) && between (p->id (), x, n->id ())) {
      p = n;
    }
    succ = n;
  }

  // use closestpred, because we pinned pred
  n = locations->closestpredloc (mydoubleID);
  if (between (myID, x, n->id ()) && between (p->id (), x, n->id ())) {
    p = n;
  }

  return p;
}

void
debruijn::stabilize ()
{
  find_successor (mydoubleID, 
		  wrap (this, &debruijn::finddoublesucc_cb));
}

void
debruijn::finddoublesucc_cb (vec<chord_node> s, route search_path, chordstat status)
{
  if (status) {   
    warnx << myID << ": finddoublesucc_cb: failure status " << status << "\n";
  } else {
    //  warnx << myID << ": finddoublesucc_cb: " << mydoubleID << " is " << s 
    //  << "\n";
  }
}
		      
void
debruijn::dodebruijn (user_args *sbp, debruijn_arg *da)
{
  debruijn_res *res;
  ptr<location> succ = my_succ ();

  //  warnx << myID << " dodebruijn: succ " << succ << " x " << da->x << " i " 
  // << da->i << " between " << betweenrightincl (myID, succ, da->i) 
  // << " k " << da->k << "\n";

  res = New debruijn_res ();
  if (betweenrightincl (myID, succ->id (), da->x) ||
      (myID == succ->id ())) {
    res->set_status(CHORD_INRANGE);
    succ->fill_node (res->inres->node);
    vec<ptr<location> > succs = successors->succs ();
    res->inres->succs.setsize (succs.size ());
    for (size_t i = 0; i < succs.size (); i++)
      succs[i]->fill_node (res->inres->succs[i]);
  } else {
    vec<chordID> ignored_failed;
    res->set_status (CHORD_NOTINRANGE);
    if (betweenrightincl (myID, succ->id (), da->i)) {
      res->noderes->k = shifttopbitout (logbase_, da->k);
      res->noderes->i = doubleID (da->i, logbase_);
      res->noderes->i = res->noderes->i | topbits (logbase_, da->k);
      ptr<location> nd = closestpred (res->noderes->i, ignored_failed);
      nd->fill_node (res->noderes->node);
    } else {
      ptr<location> x = closestpred (da->i, ignored_failed); // succ
      x->fill_node (res->noderes->node);
      res->noderes->i = da->i;
      res->noderes->k = da->k;
    }
    vec<ptr<location> > succs = successors->succs ();
    res->noderes->succs.setsize (succs.size ());
    for (size_t i = 0; i < succs.size (); i++)
      succs[i]->fill_node (res->noderes->succs[i]);
  }

  if (da->upcall_prog)  {
    do_upcall (da->upcall_prog, da->upcall_proc,
	       da->upcall_args.base (), da->upcall_args.size (),
	       wrap (this, &debruijn::debruijn_upcall_done, da, res, sbp));
    
  } else {
    sbp->reply (res);
    delete res;
  }
}

void
debruijn::debruijn_upcall_done (debruijn_arg *da,
				  debruijn_res *res,
				  user_args *sbp,
				  bool stop)
{
  if (stop) res->set_status (CHORD_STOP);
  sbp->reply(res);
  delete res;
}

void
debruijn::print (strbuf &outbuf) const
{
  // XXX maybe should call parent print.
  outbuf << "======== " << myID << " ====\n";
  outbuf << myID << ": double: " << mydoubleID
	 << " : d " << locations->closestpredloc (mydoubleID)->id () << "\n";
  successors->print (outbuf);
  outbuf << "pred : " << my_pred ()->id () << "\n";
  outbuf << "=====================================================\n";
}

void
debruijn::dispatch (user_args *sbp)
{
  if (sbp->prog->progno != debruijn_program_1.progno) {
    vnode_impl::dispatch (sbp);
    return;
  }

  if (!sbp)
    return;
  
  switch (sbp->procno) {
  case DEBRUIJNPROC_NULL:
    sbp->reply (NULL); // XXX
    break;
  case DEBRUIJNPROC_ROUTE:
    {
      debruijn_arg *da = 
	sbp->Xtmpl getarg<debruijn_arg> ();
      dodebruijn (sbp, da);
    }
    break;

    break;
  default:
    warn << "unknown proc in debruijn::dispatch " << sbp->procno << "\n";
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}


//
// de Bruijn routing 
//
ptr<route_iterator>
debruijn::produce_iterator (chordID xi)
{
  return New refcounted<route_debruijn> (mkref (this), xi, logbase_);
}


ptr<route_iterator> 
debruijn::produce_iterator (chordID xi,
			    const rpc_program &uc_prog,
			    int uc_procno,
			    ptr<void> uc_args)
{
  return New refcounted<route_debruijn> (mkref (this), xi, logbase_,
					 uc_prog, uc_procno, uc_args);
}


route_iterator *
debruijn::produce_iterator_ptr (chordID xi)
{
  return New route_debruijn (mkref (this), xi, logbase_);
}


route_iterator *
debruijn::produce_iterator_ptr (chordID xi,
				const rpc_program &uc_prog,
				int uc_procno,
				ptr<void> uc_args)
{
  return New route_debruijn (mkref (this), xi, logbase_,
			     uc_prog, uc_procno, uc_args);
}
