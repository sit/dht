#include "chord.h"
#include "location.h"
#include "route_secchord.h"

#define NSUCC 16

// XXX should timeout if outstanding is too high for too long? maybe
//     can tolerate a few failures.

//
// Secchord finger table routing.
// Works best (or at all) if finger tables additionally
// stabilize for successor lists of each finger.
//
route_secchord::route_secchord (ptr<vnode> vi, chordID xi) :
  route_iterator (vi, xi),
  lasthop_ (false), bracketed_key_ (0), outstanding_ (0) {}

route_secchord::route_secchord (ptr<vnode> vi, chordID xi,
			    rpc_program uc_prog,
			    int uc_procno,
			    ptr<void> uc_args) : 
  route_iterator (vi, xi, uc_prog, uc_procno, uc_args),
  lasthop_ (false), bracketed_key_ (0), outstanding_ (0)
{
  prog = uc_prog;
  this->uc_args = uc_args;
  this->uc_procno = uc_procno;
  warnx << v->my_ID() << ": route_secchord: upcall!\n";
}

route_secchord::~route_secchord ()
{
  clear_nexthops ();
}

void
route_secchord::clear_nexthops ()
{
  node *n;
  while ((n = nexthops_.first ()) != NULL) {
    nexthops_.remove (n->n_);
    delete n;
  }
}

bool
route_secchord::sufficient_successors ()
{
  if (nexthops_.first () == NULL)
    return false;

  // What if the key is stored on the quering node?
  // - We want the search to go all the way around the ring anyway, since
  //   our predecessor may have changed.
  
  // What if we are the only node in the whole network?
  // What if there are only two nodes in the network?
  // What if there are fewer than NSUCC nodes in the network?

  // We want to be sure that we have found the successor list of the
  // key so we must have some estimate of how many nodes are available
  // to us.  The available count at this point should include the number
  // of nodes that are in nexthops_; they ought to have been cached
  // by next_hop_cb
  size_t available = v->locations->usablenodes ();
  size_t desired = (available > NSUCC) ? NSUCC : available;

  chordID myID = v->my_ID ();
  
  node *p = nexthops_.closestpred (x);
  assert (p);
  node *s = nexthops_.next (p);
  if (s == NULL)
    s = nexthops_.first ();
  
  size_t successors = 0;

  // we must also ensure that the successors are closely packed.
  // that is, we want to have talked to someone who returned something
  // that actually included a pair of bracketing nodes, with
  // approximately the right density.
  
  /*  while (s != p && !between (p->n_, s->n_, myID)) { */
  while (s != p && successors < desired) {
    warnx << myID << ": sufficient_successors (" << x << "): s = "
	  << s->n_ << "; p = " << p->n_ << "\n";
    successors++;
    assert (betweenrightincl (p->n_, s->n_, x));
    s = nexthops_.next (s);
    if (s == NULL)
      s = nexthops_.first ();

  }
  warnx << myID << ": sufficient_successors (" << x << "): found " << successors
	<< " successors, out of " << desired << " desired.\n";
  if (s == p)
    warnx << myID << ": sufficient_successors (" << x << "): and s == p, so done.\n";
  
  return ((s == p) || (successors >= desired));
}

void
route_secchord::first_hop (cbhop_t cbi, chordID guess)
{
  // Ignore their guess.
  first_hop (cbi, false);
}

void
route_secchord::first_hop (cbhop_t cbi, bool ucs)
{
  if (ucs)
    warnx << "!!!! Use cached successors not yet supported for route_secchord !!!!";

  chordID myID = v->my_ID ();
  warnx << myID << ": STARTING a lookup for " << x
	<< (do_upcall ? "with upcall\n" :"\n");
  cb = cbi;

  clear_nexthops ();
  vec<chordID> sl = v->succs ();
  for (u_int i = 0; i < sl.size (); i++) {
    node *n = New node (sl[i], v->locations->getaddress (sl[i]));
    nexthops_.insert (n); // disregard return val; no dupes in first hop
    warnx << v->my_ID () << ": secchord::first_hop: inserting " << n->n_ << "\n";
  }
  
  next_hop ();
}

void
route_secchord::next_hop ()
{
  warnx << v->my_ID () << ": secchord::next_hop: " << x << "\n";
  node *n = nexthops_.first ();
  // Send to the first NSUCC nodes that we've heard back from.
  // XXX attempt to send to NSUCC different IP addresses?
  while (n != NULL && (outstanding_ < NSUCC)) {
    ref<chord_testandfindarg> arg = New refcounted<chord_testandfindarg> ();
    chord_nodelistres *res = New chord_nodelistres (CHORD_OK);

    arg->v = n->n_;
    arg->x = x;
    arg->failed_nodes.setsize (0); // never used.
    if (do_upcall && (lasthop_ || !outstanding_)) {
      //if (do_upcall) {
      int arglen;
      char *marshalled_args = route_iterator::marshall_upcall_args (&prog,
								    uc_procno,
								    uc_args,
								    &arglen);
      
      arg->upcall_prog = prog.progno;
      arg->upcall_proc = uc_procno;
      arg->upcall_args.setsize (arglen);
      memcpy (arg->upcall_args.base (), marshalled_args, arglen);
      delete marshalled_args;
    } else
      arg->upcall_prog = 0;
    
    outstanding_++;
    warnx << v->my_ID () << ": secchord::next_hop " << x
	  << " send secfindsucc to " << n->n_ << "\n";
    v->doRPC (n->cn_, chord_program_1, CHORDPROC_SECFINDSUCC,
	      arg, res,
	      wrap (this, &route_secchord::next_hop_cb, deleted, n->cn_, res));
    n = nexthops_.next (n);
  }
  // Now we can forget about these guys, in preparation for receiving
  // responses from the RPCs we just issued.
  if (!lasthop_)
    clear_nexthops ();
}

void
route_secchord::next_hop_cb (ptr<bool> deleted, chord_node dst,
			     chord_nodelistres *res, clnt_stat err)
{
  if (*deleted) return;
  outstanding_--;
  assert (outstanding_ <= NSUCC && outstanding_ >= 0);

  if (err) {
    // Maybe we can just ignore this guy.
    // Don't alert since we don't bother to keep track where we
    // heard about each node from.
    warnx << v->my_ID () << ": secchord::next_hop: err " << err << "\n";
  } else if (res->status != CHORD_OK) {
    warnx << v->my_ID () << ": secchord::next_hop: status " << res->status << "\n";
  } else {
    for (u_int i = 0; i < res->resok->nlist.size (); i++) {
      // Ensure that this is a valid location, and further make
      // sure that it will be counted for later use in sufficient_successors
      if (!v->locations->insert (res->resok->nlist[i]))
	continue; // XXX one bad node in reply ==> untrustworthy source??
      node *n = nexthops_.search (res->resok->nlist[i].x);
      if (!n) {
	n = New node (res->resok->nlist[i]);
	warnx << v->my_ID () <<  ": secchord::next_hop: " << x << " attempting to insert " << n->n_ << "\n";
	bool ok = nexthops_.insert (n);
	assert (ok);
      } else {
	n->count_++;
      }
    }
  }


  if (outstanding_ == 0) {
    if (nexthops_.first () == NULL) {
      warnx << "+++++ route search " << x << " got no answers.\n";
      r = CHORD_ERRNOENT;
      cb (done = true);
      return;
    }
    if (lasthop_) {
      warnx << "+++++ route search " << x << " done (after upcall)!\n";
      print ();
      cb (done = true);
      return;
    }
    bool soondone = sufficient_successors ();
    node *s = NULL;
    if (soondone) {
      if (do_upcall) {
	// XXX must deliver upcall to the successors as well.
	warnx << "+++++ route search " << x << " penultimate (b/c of upcall)!\n";
	lasthop_ = true;
      } else {
	warnx << "+++++ route search " << x << " done.\n";
	done = true;
      }

      s = nexthops_.closestsucc (x);
    } else {
      warnx << "+++++ route search " << x << " partial!\n";
      s = nexthops_.closestsucc (v->my_ID ());
    }
    // XXX only need to check to see if pushing the same last guy again?
    for (size_t i = search_path.size (); i > 0; i--)
      if (s->n_ == search_path[i - 1]) {
	warnx << "+++++ but already done??? XXX\n";
	print ();
	cb (true);
	return;
      }
    warnx << "+++++ route search " << x << " push back " << s->n_ <<"\n";
    search_path.push_back (s->n_);
    print ();

    cb (done);
  }

}

void
route_secchord::send (chordID guess)
{
  first_hop (wrap (this, &route_secchord::send_hop_cb), guess);
}

void
route_secchord::send (bool ucs)
{
  first_hop (wrap (this, &route_secchord::send_hop_cb), ucs);
}

void
route_secchord::send_hop_cb (bool done)
{
  if (!done) next_hop ();
}

chordID
route_secchord::pop_back () 
{
  return search_path.pop_back ();
}

void
route_secchord::print ()
{
  warnx << v->my_ID () << " searching for " << x << "\n";
  for (unsigned i = 0; i < search_path.size (); i++) {
    warnx << " " << search_path[i] << "\n";
  }
  warnx << "++++++\n";
}

ptr<route_iterator>
secchord_route_factory::produce_iterator (chordID xi)
{
  return New refcounted<route_secchord> (vi, xi);
}

ptr<route_iterator>
secchord_route_factory::produce_iterator (chordID xi,
					  rpc_program uc_prog,
					  int uc_procno,
					  ptr<void> uc_args)
{
  return New refcounted<route_secchord> (vi, xi, uc_prog, uc_procno, uc_args);
}

route_iterator *
secchord_route_factory::produce_iterator_ptr (chordID xi)
{
  return New route_secchord (vi, xi);
}

route_iterator *
secchord_route_factory::produce_iterator_ptr (chordID xi,
					      rpc_program uc_prog,
					      int uc_procno,
					      ptr<void> uc_args)
{
  return New route_secchord (vi, xi, uc_prog, uc_procno, uc_args);
}

