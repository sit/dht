#include "arpc.h"
#include "chord.h"
#include "route.h"
#include <misc_utils.h>
#include <location.h>
#include <locationtable.h>

void
route_iterator::print ()
{
  for (unsigned i = 0; i < search_path.size (); i++) {
    warnx << search_path[i]->id () << "\n";
  }
}

char *
route_iterator::marshall_upcall_args (rpc_program *prog, 
				      int uc_procno,
				      ptr<void> uc_args,
				      int *upcall_args_len)
{
  xdrproc_t inproc = prog->tbl[uc_procno].xdr_arg;
  xdrsuio x (XDR_ENCODE);
  if ((!inproc) || (!inproc (x.xdrp (), uc_args))) 
    return NULL;
  
  *upcall_args_len = x.uio ()->resid ();
  return suio_flatten (x.uio ());
}


bool
route_iterator::unmarshall_upcall_res (rpc_program *prog, 
				       int uc_procno, 
				       void *upcall_res,
				       int upcall_res_len,
				       void *dest)
{
  xdrmem x ((char *)upcall_res, upcall_res_len, XDR_DECODE);
  xdrproc_t proc = prog->tbl[uc_procno].xdr_res;
  assert (proc);
  if (!proc (x.xdrp (), dest)) 
    return true;
  return false;
}

//
// Finger table routing 
//

route_chord::route_chord (ptr<vnode> vi, chordID xi) : 
  route_iterator (vi, xi) {};

route_chord::route_chord (ptr<vnode> vi, chordID xi,
			  rpc_program uc_prog,
			  int uc_procno,
			  ptr<void> uc_args) : 
  route_iterator (vi, xi, uc_prog, uc_procno, uc_args)
{

  prog = uc_prog;
  this->uc_args = uc_args;
  this->uc_procno = uc_procno;

};

void
route_chord::first_hop (cbhop_t cbi, chordID guess)
{
  cb = cbi;
  ptr<location> l = v->locations->lookup (guess);
  search_path.push_back (l);
  assert (l != NULL); // XXX gross.
  next_hop ();
}

void
route_chord::first_hop (cbhop_t cbi, bool ucs)
{
  //  warn << v->my_ID () << "; starting a lookup for " << x << "\n";
  cb = cbi;

  chordID myID = v->my_ID ();
  ptr<location> succ = v->my_succ ();
  if (betweenrightincl (myID, succ->id (), x) || myID == succ->id ()) {
    search_path.push_back (succ);
    next_hop (); // deliver the upcall
  } else {
    ptr<location> n;
    if (ucs) 
      n = v->lookup_closestsucc (x);
    else
      n = v->lookup_closestpred (x);

    search_path.push_back (n);
    next_hop ();
  }
}

void
route_chord::next_hop ()
{
  ptr<location> n = search_path.back();
  //  warn << v->my_ID () << "; next_hop: " << n << " is next\n";
  make_hop(n);
}


void
route_chord::send (chordID guess)
{
  first_hop (wrap (this, &route_chord::send_hop_cb), guess);
}

void
route_chord::send (bool ucs)
{
  first_hop (wrap (this, &route_chord::send_hop_cb), ucs);
}

void
route_chord::send_hop_cb (bool done)
{
  if (!done) next_hop ();
}

void
route_chord::make_hop (ptr<location> n)
{
  ptr<chord_testandfindarg> arg = New refcounted<chord_testandfindarg> ();
  arg->x = x;
  arg->failed_nodes.setsize (failed_nodes.size ());
  for (unsigned int i = 0; i < failed_nodes.size (); i++)
    arg->failed_nodes[i] = failed_nodes[i];

  if (do_upcall) {
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

  chord_testandfindres *nres = New chord_testandfindres (CHORD_OK);
  v->doRPC (n, chord_program_1, CHORDPROC_TESTRANGE_FINDCLOSESTPRED, 
	    arg, nres, wrap (this, &route_chord::make_hop_cb, deleted, nres));

}

ptr<location>
route_chord::pop_back () 
{
  return search_path.pop_back ();
}


void
route_chord::on_failure (chordID f)
{
  failed_nodes.push_back (f);
  v->alert (search_path.back (), f);
  warn << v->my_ID () << ": " << f << " is down.  Now trying "
       << search_path.back ()->id () << "\n";
  last_hop = false;
  next_hop ();
}

void
route_chord::make_hop_cb (ptr<bool> del,
			  chord_testandfindres *res, clnt_stat err)
{
  if (*del) return;
  if (err) {
    //back up
    ptr<location> last_node_tried = pop_back ();
    if (search_path.size () == 0)
      search_path.push_back (v->my_location ());

    on_failure (last_node_tried->id ());
  } else if (res->status == CHORD_STOP) {
    r = CHORD_OK;
    cb (done = true);
  } else if (last_hop) {
    //talked to the successor
    r = CHORD_OK;
    cb (done = true);
  } else if (res->status == CHORD_INRANGE) { 
    // found the successor
    ptr<location> n0 = v->locations->insert (make_chord_node (res->inrange->n[0]));
    if (!n0) {
      warnx << v->my_ID () << ": make_hop_cb: inrange node ("
	    << res->inrange->n[0] << ") not valid vnode!\n";
      assert (0);
    }
    search_path.push_back (n0);
    successors_.clear ();
    for (size_t i = 0; i < res->inrange->n.size (); i++)
      successors_.push_back (make_chord_node (res->inrange->n[i]));
    
    last_hop = true;
    if (stop) done = true; // XXX still needed??
    cb (done);
  } else if (res->status == CHORD_NOTINRANGE) {
    // haven't found the successor yet
    ptr<location> last = search_path.back ();
    chord_node n = make_chord_node (res->notinrange->n);
    if (last->id () == n.x) {   
      warnx << v->my_ID() << ": make_hop_cb: node " << last->id ()
	   << "returned itself as best pred, looking for "
	   << x << "\n";
      r = CHORD_ERRNOENT;
      cb (done = true);
    } else {
      // make sure that the new node sends us in the right direction,
      chordID olddist = distance (search_path.back ()->id (), x);
      chordID newdist = distance (n.x, x);
      if (newdist > olddist) {
	warnx << "XXXXXXXXXXXXXXXXXXX WRONG WAY XXXXXXXXXXXXX\n";
	warnx << v->my_ID() << ": make_hop_cb: went in the wrong direction:"
	      << " looking for " << x << "\n";
	// xxx surely we can do something more intelligent here.
	print ();
	warnx << v->my_ID() << ": " << search_path.back ()->id ()
	      << " sent me to " << n.x
	      << " looking for " << x << "\n";
	warnx << "XXXXXXXXXXXXXXXXXXX WRONG WAY XXXXXXXXXXXXX\n";
      }
      
      //BAD LOC (ok)
      ptr<location> n0 = v->locations->insert (n);
      if (!n0) {
	warnx << v->my_ID () << ": make_hop_cb: notinrange node ("
	      << res->notinrange->n << ") not valid vnode!\n";
	assert (0);
      }
      search_path.push_back (n0);
      
      successors_.clear ();
      for (size_t i = 0; i < res->notinrange->succs.size (); i++)
	successors_.push_back (make_chord_node (res->notinrange->succs[i]));

      assert (search_path.size () <= 1000);
      cb (done);
    }
  } else {
    fatal << "status was unreasonable: " << res->status << "\n";
  }
  delete res;
}


ptr<route_iterator>
chord_route_factory::produce_iterator (chordID xi)
{
  return New refcounted<route_chord> (vi, xi);
}

ptr<route_iterator> 
chord_route_factory::produce_iterator (chordID xi,
				       rpc_program uc_prog,
				       int uc_procno,
				       ptr<void> uc_args)
{
  return New refcounted<route_chord> (vi, xi, uc_prog, uc_procno, uc_args);
}


route_iterator *
chord_route_factory::produce_iterator_ptr (chordID xi)
{
  return New route_chord (vi, xi);
}

route_iterator *
chord_route_factory::produce_iterator_ptr (chordID xi,
					   rpc_program uc_prog,
					   int uc_procno,
					   ptr<void> uc_args)
{
  return New route_chord (vi, xi, uc_prog, uc_procno, uc_args);
}

//
// de Bruijn routing 
//

route_debruijn::route_debruijn (ptr<vnode> vi, chordID xi) : 
  route_iterator (vi, xi), hops (0) {};

route_debruijn::route_debruijn (ptr<vnode> vi, chordID xi,
				rpc_program uc_prog,
				int uc_procno,
				ptr<void> uc_args) : 
  route_iterator (vi, xi, uc_prog, uc_procno, uc_args), hops (0)
{

  prog = uc_prog;
  this->uc_args = uc_args;
  this->uc_procno = uc_procno;

};

void
route_debruijn::print ()
{
  for (unsigned i = 0; i < search_path.size (); i++) {
    warnx << search_path[i]->id () << " " << virtual_path[i] << " " << k_path[i] 
	  << "\n";
  }
}

static chordID
createdebruijnkey (chordID p, chordID n, chordID &x, chordID *k, int logbase)
{
  // to reduce lookup time from O(b)---where b is 160---to O(N)---where
  // is the number of nodes. create r with as many top bits from x as possible,
  // but while p <= r <= n:

  // compute bm, the bit in p in which p and n mismatch
  int bm = bitindexmismatch (n, p);
  int bs = bm;

  // skip the first 0 bit in p; result will be smaller than n
  for ( ; bs >= 0; bs--) {
    if (p.getbit (bs) == 0)
      break;
  }
  bs--;

  // skip till the next 0 bit in p;
  for ( ; bs >= 0; bs--) {
    if (p.getbit (bs) == 0)
      break;
  }
  // set that bit to 1, now q is larger than p and smaller than n.
  chordID r = p;
  r.setbit (bs, 1);
  bs--;
  
  int mod = bs % logbase;
  bs = bs - mod - 1;

  if (bs >= 0) {
    // slap top bits from x at pos in r, starting with b0x 0s
    r = createbits (r, bs, x);
    // compute the remainder of key that debruijn needs to shift in
    chordID kr = shifttopbitout (bs+1, x);
    *k = kr;
  } else {
    *k = x;
  }

  //  warnx << "r = " << r << "\nn (" << n.nbits () << ")= " << n
  //<< "\np (" << p.nbits () << ")= " << p 
  //<< "\nx (" << x.nbits () << ")= " << x
  //<< "\n";
  //warnx << "k: " << *k << "\n";

  assert (betweenrightincl (p, n, r));

  return r;
}

void
route_debruijn::first_hop (cbhop_t cbi, chordID guess) {
  cb = cbi;
  k_path.push_back (x);
  ptr<location> l = v->locations->lookup (guess);
  search_path.push_back (l);
  assert (l); // XXX gross
  virtual_path.push_back (x);
  next_hop ();
}

void
route_debruijn::first_hop (cbhop_t cbi, bool ucs)
{
  cb = cbi;
  chordID myID = v->my_ID ();

  warnx << "first_hop: " << x << "\n";
  if (v->lookup_closestsucc (myID + 1)->id() == myID) {  // is myID the only node?
    done = true;
    search_path.push_back (v->my_location ());
    virtual_path.push_back (x);
    k_path.push_back (x);
  } else {
    chordID k;
    chordID r = createdebruijnkey (myID, v->my_succ ()->id (), x, &k, logbase);
    //    chordID r = myID + 1;
    search_path.push_back (v->my_location ());
    virtual_path.push_back (r);
    k_path.push_back (k);
  }
  next_hop (); //do it anyways
}


void
route_debruijn::send (chordID guess)
{
  first_hop (wrap (this, &route_debruijn::send_hop_cb), guess);
}

void
route_debruijn::send (bool ucs)
{
  first_hop (wrap (this, &route_debruijn::send_hop_cb), ucs);
}

void
route_debruijn::next_hop ()
{
  ptr<location> n = search_path.back ();
  chordID i = virtual_path.back ();
  chordID k = k_path.back ();

  make_hop (n, x, k, i);
}


void
route_debruijn::send_hop_cb (bool done)
{
  if (!done) next_hop ();
}

void
route_debruijn::make_hop (ptr<location> n, chordID &x, chordID &k, chordID &i)
{
  ptr<chord_debruijnarg> arg = New refcounted<chord_debruijnarg> ();
  arg->n = n->id ();
  arg->x = x;
  arg->i = i;
  arg->k = k;

 if (do_upcall) {
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

  chord_debruijnres *nres = New chord_debruijnres (CHORD_OK);
  v->doRPC (n, chord_program_1, CHORDPROC_DEBRUIJN, arg, nres, 
	 wrap (this, &route_debruijn::make_hop_cb, deleted, nres));
}

static int 
uniquepathsize (vec<chordID> path) {
  int n = 1;
  for (unsigned int i = 1; i < path.size (); i++) {
    if (path[i] != path[i-1]) n++;
  }
  return n;
}

void
route_debruijn::make_hop_cb (ptr<bool> del, chord_debruijnres *res, 
			     clnt_stat err)
{
  if (*del) return;
  if (err) {
    warnx << "make_hop_cb: failure " << err << "\n";
    
    delete res;
  } else if (res->status == CHORD_STOP) {
    r = CHORD_OK;
    cb (done = true);
  } else if (last_hop) {
    r = CHORD_OK;
    cb (done = true);
  } else if (res->status == CHORD_INRANGE) { 
    // found the successor
    //BAD LOC (ok)
    ptr<location> n0 = v->locations->insert (make_chord_node (res->inres->node));
    if (!n0) {
      warnx << v->my_ID () << ": debruijn::make_hop_cb: inrange node ("
	    << make_chordID (res->inres->node) << "@" << res->inres->node.r.hostname
	    << ":" << res->inres->node.r.port << ") not valid vnode!\n";
      assert (0); // XXX handle malice more intelligently
    }
    search_path.push_back (n0);
    virtual_path.push_back (x);   // push some virtual path node on
    k_path.push_back (0);  // k is shifted
    if (stop) done = true;
    last_hop = true;
    warnx << "make_hop_cb: x " << x << " path " << search_path.size() - 1
	  << " ipath " << uniquepathsize (virtual_path) - 1 << " :\n";
    print ();
    cb (done);
  } else if (res->status == CHORD_NOTINRANGE) {
    // haven't found the successor yet
    ptr<location> n0 = v->locations->insert (make_chord_node (res->noderes->node));
    if (!n0) {
      warnx << v->my_ID () << ": debruijn::make_hop_cb: inrange node ("
	    << make_chordID (res->noderes->node) << "@" << res->noderes->node.r.hostname
	    << ":" << res->noderes->node.r.port << ") not valid vnode!\n";
      assert (0); // XXX handle malice more intelligently
    }
    
    chordID i = res->noderes->i;
    chordID k = res->noderes->k;
    if (i != virtual_path.back ()) hops++;
    search_path.push_back (n0);
    virtual_path.push_back (i);
    k_path.push_back (k);
    
    if ((hops > NBIT) || (search_path.size() > 400) ) {
      warnx << "make_hop_cb: too long a search path: " << v->my_ID() 
	    << " looking for " << x << " k = " << k << "\n";
      print ();
      assert (0);
    }
    if (stop) done = true;
    cb (done);
  } else {
    warnx << "Unexpected status: " << res->status << "\n";
    assert (0);
  }
  delete res;
}

ptr<location>
route_debruijn::pop_back ()
{
  return NULL;
}


ptr<route_iterator>
debruijn_route_factory::produce_iterator (chordID xi)
{
  return New refcounted<route_debruijn> (vi, xi);
}


ptr<route_iterator> 
debruijn_route_factory::produce_iterator (chordID xi,
					  rpc_program uc_prog,
					  int uc_procno,
					  ptr<void> uc_args)
{
  return New refcounted<route_debruijn> (vi, xi, uc_prog, uc_procno, uc_args);
}


route_iterator *
debruijn_route_factory::produce_iterator_ptr (chordID xi)
{
  return New route_debruijn (vi, xi);
}


route_iterator *
debruijn_route_factory::produce_iterator_ptr (chordID xi,
					      rpc_program uc_prog,
					      int uc_procno,
					      ptr<void> uc_args)
{
  return New route_debruijn (vi, xi, uc_prog, uc_procno, uc_args);
}

void 
route_factory::get_node (chord_node_wire *n) {
  vi->locations->lookup (vi->my_ID ())->fill_node (*n); 
}
