/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 * Copyright (C) 2001 Frans Kaashoek (kaashoek@lcs.mit.edu) and 
 *                    Frank Dabek (fdabek@lcs.mit.edu).
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include <assert.h>
#include <qhash.h>

#include "chord_impl.h"
#include <location.h>

#include "stabilize.h"
#include "succ_list.h"
#include "pred_list.h"

#include "comm.h"
#include <coord.h>
#include <modlogger.h>
#include <misc_utils.h>
#include <id_utils.h>
#include <locationtable.h>

#include <configurator.h>

ref<vnode>
vnode::produce_vnode (ref<chord> _chordnode,
		      ref<rpc_manager> _rpcm,
		      ref<location> _l)
{
  return New refcounted<vnode_impl> (_chordnode, _rpcm, _l);
}

// Pure virtual destructors still need definitions
vnode::~vnode () {}

chordID
vnode_impl::my_ID () const
{
  return myID;
}

ref<location>
vnode_impl::my_location ()
{
  return me_;
}

vnode_impl::vnode_impl (ref<chord> _chordnode,
			ref<rpc_manager> _rpcm,
			ref<location> _l) :
  me_ (_l),
  rpcm (_rpcm),
  myindex (_l->vnode ()),
  myID (_l->id ()), 
  chordnode (_chordnode)
{
  locations = _chordnode->locations;

  warnx << gettime () << " myID is " << myID << "\n";

  successors = New refcounted<succ_list> (mkref(this), locations);
  predecessors = New refcounted<pred_list> (mkref(this), locations);
  stabilizer = New refcounted<stabilize_manager> (myID);

  stabilizer->register_client (successors);
  stabilizer->register_client (predecessors);

  locations->incvnodes ();

  addHandler (chord_program_1, wrap(this, &vnode_impl::dispatch));

  ngetsuccessor = 0;
  ngetpredecessor = 0;
  ngetsucclist = 0;
  nfindsuccessor = 0;
  nhops = 0;
  nmaxhops = 0;
  nfindpredecessor = 0;
  nnotify = 0;
  nalert = 0;
  
  ndogetsuccessor = 0;
  ndogetpredecessor = 0;
  ndonotify = 0;
  ndoalert = 0;
  ndogetsucclist = 0;
  ndogetsucc_ext = 0;
  ndogetpred_ext = 0;

  assert (Configurator::only ().get_int ("chord.checkdead_interval", checkdead_int));
}

vnode_impl::~vnode_impl ()
{
  warnx << myID << ": vnode_impl: destroyed\n";
  exit (0);
}

void
vnode_impl::dispatch (user_args *a)
{
  switch (a->procno) {
  case CHORDPROC_NULL: 
    {
      a->reply (NULL);
    }
    break;
  case CHORDPROC_GETSUCCESSOR:
    {
      doget_successor (a);
    }
    break;
  case CHORDPROC_GETPREDECESSOR:
    {
      doget_predecessor (a);
    }
    break;
  case CHORDPROC_NOTIFY:
    {
      chord_nodearg *na = a->Xtmpl getarg<chord_nodearg> ();
      donotify (a, na);
    }
    break;
  case CHORDPROC_ALERT:
    {
      chord_nodearg *na = a->Xtmpl getarg<chord_nodearg> ();
      doalert (a, na);
    }
    break;
  case CHORDPROC_GETSUCCLIST:
    {
      dogetsucclist (a);
    }
    break;
  case CHORDPROC_GETPREDLIST:
    {
      dogetpredlist (a);
    }
    break;
  case CHORDPROC_TESTRANGE_FINDCLOSESTPRED:
    {
      chord_testandfindarg *fa = a->Xtmpl getarg<chord_testandfindarg> ();
      doroute (a, fa);
    }
    break;
  case CHORDPROC_GETPRED_EXT:
    {
      dogetpred_ext (a);
    }
    break;
  case CHORDPROC_GETSUCC_EXT:
    {
      dogetsucc_ext (a);
    }
    break;
  case CHORDPROC_FINDROUTE:
    {
      chord_findarg *fa = a->Xtmpl getarg<chord_findarg> ();
      dofindroute (a, fa);
    }
    break;
  default:
    a->reject (PROC_UNAVAIL);
    break;
  }
}

ptr<location>
vnode_impl::my_pred() const
{
  return predecessors->pred ();
}

ptr<location>
vnode_impl::my_succ () const
{
  return successors->succ ();
}

void
vnode_impl::stats () const
{
  warnx << "VIRTUAL NODE STATS " << myID
	<< " stable? " << stabilizer->isstable () << "\n";
  warnx << "# estimated node in ring "
	<< locations->estimate_nodes () << "\n";
  warnx << "continuous_timer " << stabilizer->cts_timer ()
	<< " backoff " 	<< stabilizer->bo_timer () << "\n";
  
  warnx << "# getsuccesor requests " << ndogetsuccessor << "\n";
  warnx << "# getpredecessor requests " << ndogetpredecessor << "\n";
  warnx << "# getsucclist requests " << ndogetsucclist << "\n";
  warnx << "# notify requests " << ndonotify << "\n";  
  warnx << "# alert requests " << ndoalert << "\n";  

  warnx << "# getsuccesor calls " << ngetsuccessor << "\n";
  warnx << "# getpredecessor calls " << ngetpredecessor << "\n";
  warnx << "# getsucclist calls " << ngetsucclist << "\n";
  warnx << "# findsuccessor calls " << nfindsuccessor << "\n";
  warnx << "# hops for findsuccessor " << nhops << "\n";
  {
    char buf[100];
    if (nfindsuccessor) {
      sprintf (buf, "   Average # hops: %f\n", ((float) nhops)/nfindsuccessor);
      warnx << buf;
    }
  }
  warnx << "   # max hops for findsuccessor " << nmaxhops << "\n";
  warnx << "# findpredecessor calls " << nfindpredecessor << "\n";
  warnx << "# notify calls " << nnotify << "\n";  
  warnx << "# alert calls " << nalert << "\n";  
}

void
vnode_impl::print (strbuf &outbuf) const
{
  outbuf << "======== " << myID << " ====\n";
  successors->print (outbuf);
  outbuf << "pred : " << my_pred ()->id () << "\n";
  outbuf << "=====================================================\n";
}

void
vnode_impl::stabilize (void)
{
  stabilizer->start ();
}

void
vnode_impl::join (ptr<location> n, cbjoin_t cb)
{
  ptr<chord_findarg> fa = New refcounted<chord_findarg> ();
  fa->x = decID (myID);
  fa->return_succs = true;
  chord_nodelistres *route = New chord_nodelistres ();
  doRPC (n, chord_program_1, CHORDPROC_FINDROUTE, fa, route,
	 wrap (this, &vnode_impl::join_getsucc_cb, n, cb, route));
}

void 
vnode_impl::join_getsucc_cb (ptr<location> n,
			     cbjoin_t cb, chord_nodelistres *route,
			     clnt_stat err)
{
  ptr<vnode> v = NULL;
  chordstat status = CHORD_OK;
  
  if (err) {
    warnx << myID << ": join RPC failed: " << err << "\n";
    if (err == RPC_TIMEDOUT) {
      // try again. XXX limit the number of retries??
      join (n, cb);
      delete route;
      return;
    }
    status = CHORD_RPCFAILURE;
  } else if (route->status != CHORD_OK) {
    status = route->status;
  } else if (route->resok->nlist.size () < 1) {
    status = CHORD_ERRNOENT;
  } else {
    // Just insert a possible predecessor and successor.
    size_t i = route->resok->nlist.size ();
    for (size_t j = 0; j < i; j++) {
      warn << my_ID () << " adding " << make_chordID(route->resok->nlist[j]) 
	<< "," << route->resok->nlist[j].knownup << "," << route->resok->nlist[j].age 
	<< " as an initial node\n";
      if (make_chordID (route->resok->nlist[j]) != my_ID ()) 
	  locations->insert (make_chord_node (route->resok->nlist[j]));
    }
    stabilize ();
    notify (my_succ (), myID);
    v = mkref (this);
    status = CHORD_OK;
  }

  if (status != CHORD_OK) {
    warnx << myID << ": join failed, remove from vnodes?\n";

    // continue to run, even if join has failed... maybe
    // stabilization (with other vnodes) will fix us over time.
    stabilize ();
    v = mkref (this);
  }
  cb (v, status);
  delete route;
}

void
vnode_impl::doget_successor (user_args *sbp)
{
  ndogetsuccessor++;
  
  ptr<location> s = my_succ ();
  chord_noderes res(CHORD_OK);
  s->fill_node (*res.resok);
  sbp->reply (&res);
}

void
vnode_impl::doget_predecessor (user_args *sbp)
{
  ndogetpredecessor++;
  ptr<location> p = my_pred ();
  chord_noderes res(CHORD_OK);
  p->fill_node (*res.resok);
  sbp->reply (&res);
}

void
vnode_impl::do_upcall_cb (char *a, int upcall_prog, int upcall_proc,
			  cbupcalldone_t done_cb, bool v)
{
  const rpc_program *prog = chordnode->get_program (upcall_prog);
  assert (prog);
  xdr_delete (prog->tbl[upcall_proc].xdr_arg, a);
  done_cb (v);
}

void
vnode_impl::do_upcall (int upcall_prog, int upcall_proc,
		  void *uc_args, int uc_args_len,
		  cbupcalldone_t done_cb)

{
  upcall_record *uc = upcall_table[upcall_prog];
  if (!uc) { 
    warn << "upcall not registered\n";
    done_cb (false);
    return;
  }

  const rpc_program *prog = chordnode->get_program (upcall_prog);
  if (!prog) 
    fatal << "bad prog: " << upcall_prog << "\n";

  
  xdrmem x ((char *)uc_args, uc_args_len, XDR_DECODE);
  xdrproc_t proc = prog->tbl[upcall_proc].xdr_arg;
  assert (proc);
  
  char *unmarshalled_args = (char *)prog->tbl[upcall_proc].alloc_arg ();
  if (!proc (x.xdrp (), unmarshalled_args))
    fatal << "upcall: error unmarshalling arguments\n";
  
  //run the upcall. It returns a pointer to its result and a length in the cb
  cbupcall_t cb = uc->cb;
  (*cb)(upcall_proc, (void *)unmarshalled_args,
	wrap (this, &vnode_impl::do_upcall_cb, 
	      unmarshalled_args, upcall_prog, upcall_proc,
	      done_cb));

}

void
vnode_impl::doroute (user_args *sbp, chord_testandfindarg *fa)
{
  chordID x = fa->x;
  chordID succ = my_succ ()->id ();

  chord_testandfindres *res = New chord_testandfindres ();  
  if (betweenrightincl(myID, succ, x) ) {
    res->set_status (CHORD_INRANGE);
    vec<ptr<location> > succs = successors->succs ();
    res->inrange->n.setsize (succs.size ());
    for (size_t i = 0; i < succs.size (); i++)
      succs[i]->fill_node (res->inrange->n[i]);
  } else {
    res->set_status (CHORD_NOTINRANGE);
    vec<chordID> f;
    for (unsigned int i=0; i < fa->failed_nodes.size (); i++)
      f.push_back (fa->failed_nodes[i]);

    ptr<location> p = closestpred (fa->x, f);
    p->fill_node (res->notinrange->n);
    
    vec<ptr<location> > succs = successors->succs ();
    res->notinrange->succs.setsize (succs.size ());
    for (size_t i = 0; i < succs.size (); i++)
      succs[i]->fill_node (res->notinrange->succs[i]);
  }

  if (fa->upcall_prog)  {
    do_upcall (fa->upcall_prog, fa->upcall_proc,
	       fa->upcall_args.base (), fa->upcall_args.size (),
	       wrap (this, &vnode_impl::upcall_done, fa, res, sbp));

  } else {
    sbp->reply (res);
    delete res;
  }
}


void
vnode_impl::upcall_done (chord_testandfindarg *fa,
			 chord_testandfindres *res,
			 user_args *sbp,
			 bool stop)
{
  if (stop) res->set_status (CHORD_STOP);
  sbp->reply (res);
  delete res;
}

void
vnode_impl::donotify (user_args *sbp, chord_nodearg *na)
{
  ndonotify++;

  predecessors->update_pred (make_chord_node (na->n));
  chordstat res = CHORD_OK;
  sbp->reply (&res);
}

void
vnode_impl::doalert (user_args *sbp, chord_nodearg *na)
{
  ndoalert++;
  chord_node n = make_chord_node (na->n);
  ptr<location> l = locations->lookup (n.x);
  if (l) {
    // check whether we cannot reach x either
    chord_noderes *res = New chord_noderes (CHORD_OK);
    ptr<chordID> v = New refcounted<chordID> (n.x);
    doRPC (l, chord_program_1, CHORDPROC_GETSUCCESSOR, v, res,
	   wrap (mkref (this), &vnode_impl::doalert_cb, res, n.x));
  }
  chordstat res = CHORD_OK;
  sbp->reply (&res);
}

void
vnode_impl::doalert_cb (chord_noderes *res, chordID x, clnt_stat err)
{
  if (!err) {
    warnx << "doalert_cb: " << x << " is alive\n";
  } else {
    warnx << "doalert_cb: " << x << " is indeed not alive\n";
    // doRPCcb has already killed this node for us.
  }
  delete res;
}

void
vnode_impl::dogetsucc_ext (user_args *sbp)
{
  chord_nodelistextres res(CHORD_OK);
  ndogetsucc_ext++;
  successors->fill_nodelistresext (&res);
  sbp->reply (&res);
}

void
vnode_impl::dogetpred_ext (user_args *sbp)
{
  ndogetpred_ext++;
  chord_nodelistextres res(CHORD_OK);
  predecessors->fill_nodelistresext (&res);
  sbp->reply (&res);
}

void
vnode_impl::dogetsucclist (user_args *sbp)
{
  ndogetsucclist++;
  chord_nodelistres res (CHORD_OK);
  successors->fill_nodelistres (&res);
  sbp->reply (&res);
}

void
vnode_impl::dogetpredlist (user_args *sbp)
{
  ndogetpredlist++;
  chord_nodelistres res (CHORD_OK);
  predecessors->fill_nodelistres (&res);
  sbp->reply (&res);
}

void
vnode_impl::dofindroute (user_args *sbp, chord_findarg *fa)
{
  find_route (fa->x, wrap (this, &vnode_impl::dofindroute_cb, sbp, fa));
}

void
vnode_impl::dofindroute_cb (user_args *sbp, chord_findarg *fa, 
			    vec<chord_node> s, route r, chordstat err)
{
  if (err) {
    chord_nodelistres res (CHORD_RPCFAILURE);
    sbp->reply (&res);
  } else {
    chord_nodelistres res (CHORD_OK);
    int nnodes_returned = r.size ();
    if (fa->return_succs) nnodes_returned += s.size ();
    res.resok->nlist.setsize (nnodes_returned);

    unsigned int n = 0;
    if (fa->return_succs) 
      for (unsigned int i = 0; i < s.size (); i++, n++) {
    	ptr<location> l = New refcounted<location> (s[i]);
    	l->fill_node (res.resok->nlist[n]);
      }
    
    for (unsigned int i = 0; i < r.size (); i++, n++) {
      r[i]->fill_node (res.resok->nlist[n]);
    }
    
    sbp->reply (&res);
  }
}

void
vnode_impl::stop (void)
{
  stabilizer->stop ();
}

vec<ptr<location> > 
vnode_impl::succs ()
{
  return successors->succs ();
}

vec<ptr<location> >
vnode_impl::preds ()
{ 
  return predecessors->preds ();
}

ptr<location>
vnode_impl::closestpred (const chordID &x, const vec<chordID> &f)
{
  return successors->closestpred (x, f);
}

ptr<route_iterator>
vnode_impl::produce_iterator (chordID xi)
{
  return New refcounted<route_chord> (mkref (this), xi);
}

ptr<route_iterator>
vnode_impl::produce_iterator (chordID xi, const rpc_program &uc_prog,
			      int uc_procno, ptr<void> uc_args)
{
  return New refcounted<route_chord> (mkref (this),
				      xi, uc_prog, uc_procno, uc_args);
}

route_iterator *
vnode_impl::produce_iterator_ptr (chordID xi)
{
  return New route_chord (mkref (this), xi);
}

route_iterator *
vnode_impl::produce_iterator_ptr (chordID xi, const rpc_program &uc_prog,
				  int uc_procno, ptr<void> uc_args)
{
  return New route_chord (mkref (this), xi, uc_prog, uc_procno, uc_args);
}
