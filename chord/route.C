#include "route.h"

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
  route_iterator (vi, xi), do_upcall (false), last_hop (false) {};

route_chord::route_chord (ptr<vnode> vi, chordID xi,
			  rpc_program uc_prog,
			  int uc_procno,
			  ptr<void> uc_args) : 
  route_iterator (vi, xi), do_upcall (true), last_hop (false)
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
	    arg, nres, wrap (this, &route_chord::make_hop_cb, nres));

}


void
route_chord::make_hop_cb (chord_testandfindres *res, clnt_stat err)
{
  if (res->status == CHORD_INRANGE)
    stop = res->inrange->stop;
  else
    stop = res->notinrange->stop;

  if (err) {
    warnx << "make_hop_cb: failure " << err << "\n";
    r = CHORD_RPCFAILURE;
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
    assert (0); // XXX handle malice more intelligently
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
    if (search_path.size () >= 1000) {
      warnx << "make_hop_done_cb: too long a search path: " << v->my_ID() 
	    << " looking for " << x << "\n";
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
	  << s << " failed. (chordstat " << status << ")\n";
    assert (0); // XXX handle malice more intelligently
  }
  if (stop) done = true;
  cb (done);
}


//
// de Bruijn routing 
//


void
route_debruijn::print ()
{
  for (unsigned i = 0; i < search_path.size (); i++) {
    warnx << search_path[i] << " " << virtual_path[i] << "\n";
  }
}

static chordID
createdebruijnkey (chordID p, chordID n, chordID &x)
{
  // to reduce lookup time from O(b)---where b is 160---to O(N)---where
  // is the number of nodes. create r with as many top bits from x as possible,
  // but while p <= r <= n:
  // 1. walk down n and p while they match
  // 2. skip 0
  // 3. walk until b0x+1 0s
  // 4. slap in top i bits of x

  // compute bm, the bit in which p and n mismatch
  int bm = bitindexmismatch (n, p);

  // skip the first 0 bit in p; result will be smaller than n
  for ( ; bm >= 0; bm--) {
    if (p.getbit (bm) == 0)
      break;
  }
  bm--;

  // compute b0x, the number of leading 0s in x
  int b0x = NBIT - x.nbits ();

  // find a b0x + 1 zero bits in p starting from bm
  int b0 = bitindexzeros (p, bm, b0x + 1);
  b0 = b0 + 1;
  //  warnx << "b0 = " << b0 << "\n";

  // slap top bits from x at b0 in p
  chordID r = createbits (p+1, b0, x);
  //  warnx << "r = " << r << "\nn = " << n
  // << "\np = " << p << "\nx = " << x << "\n";
  assert (betweenrightincl (p, n, r));
  return r;
}

void
route_debruijn::first_hop (cbhop_t cbi)
{
  cb = cbi;
  chordID myID = v->my_ID ();

  if (v->lookup_closestsucc (myID + 1) 
      == myID) {  // is myID the only node?
    done = true;
    search_path.push_back (myID);
    virtual_path.push_back (x);
    cb (done);
  } else {
    chordID r = createdebruijnkey (myID, v->my_succ (), x);
    search_path.push_back (myID);
    virtual_path.push_back (r);
    next_hop ();
  }
}

void
route_debruijn::next_hop ()
{
  chordID n = search_path.back ();
  chordID d = virtual_path.back ();
  make_hop (n, x, d);
}

void
route_debruijn::make_hop (chordID &n, chordID &x, chordID &d)
{
  ptr<chord_debruijnarg> arg = New refcounted<chord_debruijnarg> ();
  arg->v = n;
  arg->x = x;
  arg->d = d;
  chord_debruijnres *nres = New chord_debruijnres (CHORD_OK);
  v->doRPC (n, chord_program_1, CHORDPROC_DEBRUIJN, arg, nres, 
	 wrap (this, &route_debruijn::make_hop_cb, nres));
}

void
route_debruijn::make_hop_cb (chord_debruijnres *res, clnt_stat err)
{
  if (err) {
    warnx << "make_hop_cb: failure " << err << "\n";
    r = CHORD_RPCFAILURE;
    done = 1;
    cb (done);
  } else if (res->status == CHORD_INRANGE) { 
    // found the successor
    v->locations->cacheloc (res->inres->x, res->inres->r,
			    wrap (this, &route_debruijn::make_route_done_cb));
  } else if (res->status == CHORD_NOTINRANGE) {
    // haven't found the successor yet
    v->locations->cacheloc (res->noderes->node.x, res->noderes->node.r,
			    wrap (this, &route_debruijn::make_hop_done_cb,
				  res->noderes->d));
  } else {
    warn("WTF");
  }
  delete res;
}


void
route_debruijn::make_route_done_cb (chordID s, bool ok, 
				    chordstat status)
{
  if (ok && status == CHORD_OK) {
    search_path.push_back (s);
    virtual_path.push_back (x);   // push some virtual path node on
  } else if (status == CHORD_RPCFAILURE) {
    // xxx? should we retry locally before failing all the way to
    //      the top-level?
    r = CHORD_RPCFAILURE;
  } else {
    warnx << v->my_ID () << ": make_route_done_cb: last challenge for "
	  << s << " failed. (chordstat " << status << ")\n";
    assert (0); // XXX handle malice more intelligently
  }
  done = true;
  cb (done);
}

void
route_debruijn::make_hop_done_cb (chordID d, chordID s, bool ok, 
				  chordstat status)
{
  if (ok && status == CHORD_OK) {
    if (d != virtual_path.back ()) hops++;
    search_path.push_back (s);
    virtual_path.push_back (d);
    if (hops > 160) {
      warnx << "make_hop_done_cb: too long a search path: " << v->my_ID() 
	    << " looking for " << x << "\n";
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
	  << s << " failed. (chordstat " << status << ")\n";
    assert (0); // XXX handle malice more intelligently
  }
  cb (done);
}


