#include "arpc.h"
#include "chord.h"
#include "route.h"
#include "location.h"

void
route_iterator::print ()
{
  for (unsigned i = 0; i < search_path.size (); i++) {
    warnx << search_path[i] << "\n";
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
  search_path.push_back (guess);
  next_hop ();
}

void
route_chord::first_hop (cbhop_t cbi, bool ucs)
{
  cb = cbi;
  if (v->lookup_closestsucc (v->my_ID () + 1) 
      == v->my_ID ()) {  // is myID the only node?
    search_path.push_back (v->my_ID ());
    next_hop (); //do it anyways
  } else {
    chordID n;
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
  chordID n = search_path.back();
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
route_chord::make_hop (chordID &n)
{
  ptr<chord_testandfindarg> arg = New refcounted<chord_testandfindarg> ();
  arg->v = n;
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

chordID
route_chord::pop_back () 
{
  return search_path.pop_back ();
}


void
route_chord::make_hop_cb (ptr<bool> del,
			  chord_testandfindres *res, clnt_stat err)
{
  if (*del) return;
  if (err) {
    //back up
    chordID last_node_tried = pop_back ();
    if (search_path.size () == 0) search_path.push_back (v->my_ID ());
    failed_nodes.push_back (last_node_tried);
    //chordID who_told_me = peek_back ();
    //ask who_told_me for a new hint 
    warn << v->my_ID () << ": " << last_node_tried << " is down. ";
    warn << "alerting " << search_path.back () << "\n";
    v->alert (search_path.back (), last_node_tried);
    warn << " Now trying " << search_path.back () << "\n";
    next_hop ();

  } else if (res->status == CHORD_STOP) {
    r = CHORD_OK;
    cb (done = true);
  } else if (last_hop) {
    //talked to the successor
    r = CHORD_OK;
    cb (done = true);
  } else if (res->status == CHORD_INRANGE) { 
    // found the successor
    v->locations->cacheloc (res->inrange->n.x, res->inrange->n.r,
			    wrap (this, &route_chord::make_route_done_cb));
  } else if (res->status == CHORD_NOTINRANGE) {
    // haven't found the successor yet
    chordID last = search_path.back ();
    if (last == res->notinrange->n.x) {   
      // last returns itself as best predecessor, but doesn't know
      // what its immediate successor is---higher layer should use
      // succlist to make forward progress
      warn << "node returned itself as best pred\n";
      r = CHORD_ERRNOENT;
      cb (done = true);
    } else {
      // make sure that the new node sends us in the right direction,
      chordID olddist = distance (search_path.back (), x);
      chordID newdist = distance (res->notinrange->n.x, x);
      if (newdist > olddist) {
	warnx << "PROBLEM: went in the wrong direction: " << v->my_ID ()
	      << "looking for " << x << "\n";
	// xxx surely we can do something more intelligent here.
	print ();
	assert (0);
      }
      
      // ask the new node for its best predecessor
      v->locations->cacheloc (res->notinrange->n.x, res->notinrange->n.r,
			      wrap (this, &route_chord::make_hop_done_cb));
    }
  } else {
    warn("WTF");
  }
  delete res;
}


void
route_chord::make_route_done_cb (chordID s, bool ok, chordstat status)
{
  if (ok && status == CHORD_OK) {
    search_path.push_back (s);
  } else if (status == CHORD_RPCFAILURE) {
    r = CHORD_RPCFAILURE;
  } else {
    warnx << v->my_ID () << ": make_route_done_cb: last challenge for "
	  << s << " failed. (chordstat " << status << ")\n";
    r = CHORD_RPCFAILURE;
  }
  if (stop) done = true;
  last_hop = true;
  cb (done);
}

void
route_chord::make_hop_done_cb (chordID s, bool ok, chordstat status)
{
  if (ok && status == CHORD_OK) {
    search_path.push_back (s);
    assert (search_path.size () <= 1000);
  } else if (status == CHORD_RPCFAILURE) {
    // xxx? should we retry locally before failing all the way to
    //      the top-level?
    r = CHORD_RPCFAILURE;
    done = true;
  } else {
    warnx << v->my_ID () << ": make_hop_done_cb: step challenge for "
	  << s << " failed. (chordstat " << status << ")\n";
    assert (0); // XXX handle malice more intelligently
  }
  cb (done);
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
    warnx << search_path[i] << " " << virtual_path[i] << " " << k_path[i] 
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
  search_path.push_back (guess);
  virtual_path.push_back (x);
  next_hop ();
}

void
route_debruijn::first_hop (cbhop_t cbi, bool ucs)
{
  cb = cbi;
  chordID myID = v->my_ID ();

  warnx << "first_hop: " << x << "\n";
  if (v->lookup_closestsucc (myID + 1) == myID) {  // is myID the only node?
    done = true;
    search_path.push_back (myID);
    virtual_path.push_back (x);
    k_path.push_back (x);
  } else {
    chordID k;
    chordID r = createdebruijnkey (myID, v->my_succ (), x, &k, logbase);
    //    chordID r = myID + 1;
    search_path.push_back (myID);
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
  chordID n = search_path.back ();
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
route_debruijn::make_hop (chordID &n, chordID &x, chordID &k, chordID &i)
{
  ptr<chord_debruijnarg> arg = New refcounted<chord_debruijnarg> ();
  arg->n = n;
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

void
route_debruijn::make_hop_cb (ptr<bool> del, chord_debruijnres *res, 
			     clnt_stat err)
{
  if (*del) return;
  if (err) {
    warnx << "make_hop_cb: failure " << err << "\n";
    r = CHORD_RPCFAILURE;
    cb (done = true);
    delete res;
  } else if (res->status == CHORD_STOP) {
    r = CHORD_OK;
    cb (done = true);
  } else if (last_hop) {
    r = CHORD_OK;
    cb (done = true);
  } else if (res->status == CHORD_INRANGE) { 
    // found the successor
    v->locations->cacheloc (res->inres->node.x, res->inres->node.r,
			    wrap (this, &route_debruijn::make_route_done_cb));
  } else if (res->status == CHORD_NOTINRANGE) {
    // haven't found the successor yet
    v->locations->cacheloc (res->noderes->node.x, res->noderes->node.r,
			    wrap (this, &route_debruijn::make_hop_done_cb,
				  res->noderes->i, res->noderes->k));
  } else {
    warn("WTF");
  }
  delete res;
}


static int 
uniquepathsize (route path) {
  int n = 1;
  for (unsigned int i = 1; i < path.size (); i++) {
    if (path[i] != path[i-1]) n++;
  }
  return n;
}

void
route_debruijn::make_route_done_cb (chordID d, bool ok, chordstat status)
{
  if (ok && status == CHORD_OK) {
    search_path.push_back (d);     // d is the next debruijn hop
    virtual_path.push_back (x);   // push some virtual path node on
    k_path.push_back (0);  // k is shifted
  } else if (status == CHORD_RPCFAILURE) {
    // xxx? should we retry locally before failing all the way to
    //      the top-level?
    r = CHORD_RPCFAILURE;
  } else {
    warnx << v->my_ID () << ": make_route_done_cb: last challenge for "
	  << d << " failed. (chordstat " << status << ")\n";
    assert (0); // XXX handle malice more intelligently
  }
  if (stop) done = true;
  last_hop = true;
  warnx << "make_route_done_cb: x " << x << " path " << search_path.size() - 1
	<< " ipath " << uniquepathsize (virtual_path) - 1 << " :\n";
  print ();
  cb (done);
}

void
route_debruijn::make_hop_done_cb (chordID i, chordID k,
				  chordID d, bool ok, chordstat status)
{
  if (ok && status == CHORD_OK) {
    if (i != virtual_path.back ()) hops++;
    search_path.push_back (d);
    virtual_path.push_back (i);
    k_path.push_back (k);

    if ((hops > 160) || (search_path.size() > 400) ) {
      warnx << "make_hop_done_cb: too long a search path: " << v->my_ID() 
	    << " looking for " << x << " k = " << k << "\n";
      print ();
      assert (0);
    }
  } else if (status == CHORD_RPCFAILURE) {
    // xxx? should we retry locally before failing all the way to
    //      the top-level?
    r = CHORD_RPCFAILURE;
    done = true;
  } else {
    warnx << v->my_ID () << ": make_hop_done_cb: step challenge for "
	  << d << " failed. (chordstat " << status << ")\n";
    assert (0); // XXX handle malice more intelligently
  }
  if (stop) done = true;
  cb (done);
}


chordID
route_debruijn::pop_back ()
{
  return bigint (0);
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
route_factory::get_node (chord_node *n) {
  vi->locations->get_node (vi->my_ID (), n); 
};
