#include "route.h"

void
route_iterator::print ()
{
  for (unsigned i = 0; i < search_path.size (); i++) {
    warnx << search_path[i] << "\n";
  }
}


//
// Finger table routing 
//

void
route_chord::first_hop (cbhop_t cbi)
{
  cb = cbi;
  search_path.push_back (v->my_ID ());
  if (v->lookup_closestsucc (v->my_ID () + 1) 
      == v->my_ID ()) {  // is myID the only node?
    done = true;
    cb (done);
  } else {
    chordID n = v->lookup_closestpred (x);
    if ((n == v->my_ID()) && (x == v->my_ID())) {
      print ();
      assert (0);
    }
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
route_chord::make_hop (chordID &n)
{
  ptr<chord_testandfindarg> arg = New refcounted<chord_testandfindarg> ();
  arg->v = n;
  arg->x = x;
  chord_testandfindres *nres = New chord_testandfindres (CHORD_OK);
  v->doRPC (n, chord_program_1, CHORDPROC_TESTRANGE_FINDCLOSESTPRED, arg, nres, 
	 wrap (this, &route_chord::make_hop_cb, nres));

}

void
route_chord::make_hop_cb (chord_testandfindres *res, clnt_stat err)
{
  if (err) {
    warnx << "make_hop_cb: failure " << err << "\n";
    r = CHORD_RPCFAILURE;
    done = 1;
    cb (done);
  } else if (res->status == CHORD_INRANGE) { 
    // found the successor
    v->locations->cacheloc (res->inres->x, res->inres->r,
			 wrap (this, &route_chord::make_route_done_cb));
  } else if (res->status == CHORD_NOTINRANGE) {
    // haven't found the successor yet
    chordID last = search_path.back ();
    if (last == res->noderes->x) {   
      // last returns itself as best predecessor, but doesn't know
      // what its immediate successor is---higher layer should use
      // succlist to make forward progress
      r = CHORD_ERRNOENT;
      done = true;
      cb (done);
    } else {
      // make sure that the new node sends us in the right direction,
      chordID olddist = distance (search_path.back (), x);
      chordID newdist = distance (res->noderes->x, x);
      if (newdist > olddist) {
	warnx << "PROBLEM: went in the wrong direction: " << v->my_ID ()
	      << "looking for " << x << "\n";
	// xxx surely we can do something more intelligent here.
	print ();
	assert (0);
      }
      
      // ask the new node for its best predecessor
      v->locations->cacheloc (res->noderes->x, res->noderes->r,
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
  cb (done);
}


//
// de Bruijn routing 
//


void
route_debruijn::first_hop (cbhop_t cbi)
{
  cb = cbi;
  search_path.push_back (v->my_ID ());
  if (v->lookup_closestsucc (v->my_ID () + 1) 
      == v->my_ID ()) {  // is myID the only node?
    done = true;
    cb (done);
  } else {
    chordID n = v->lookup_closestsucc (x);
    search_path.push_back (n);
    virtual_path.push_back (x);
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
    search_path.push_back (s);
    virtual_path.push_back (d);
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
  cb (done);
}
