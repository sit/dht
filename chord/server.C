#include <chord_util.h>
#include <chord.h>
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

void 
vnode::get_successor (chordID n, cbchordID_t cb)
{
  //  warn << "get successor of " << n << "\n";
  ngetsuccessor++;
  chord_noderes *res = New chord_noderes (CHORD_OK);
  ptr<chord_vnode> v = New refcounted<chord_vnode>;
  v->n = n;
  warnt("CHORD: issued_GET_SUCCESSOR");
  doRPC (n, chord_program_1, CHORDPROC_GETSUCCESSOR, v, res,
	 wrap (mkref (this), &vnode::get_successor_cb, n, cb, res));
}

void
vnode::get_successor_cb (chordID n, cbchordID_t cb, chord_noderes *res, 
		       clnt_stat err) 
{
  if (err) {
    net_address dr;
    //    warnx << "get_successor_cb: RPC failure " << err << "\n";
    cb (n, dr, CHORD_RPCFAILURE);
  } else if (res->status) {
    net_address dr;
    // warnx << "get_successor_cb: RPC error " << res->status << "\n";
    cb (n, dr, res->status);
  } else {
    cb (res->resok->node, res->resok->r, CHORD_OK);
  }
  delete res;
}

void 
vnode::get_predecessor (chordID n, cbchordID_t cb)
{
  ptr<chord_vnode> v = New refcounted<chord_vnode>;
  v->n = n;
  ngetpredecessor++;
  chord_noderes *res = New chord_noderes (CHORD_OK);
  doRPC (n, chord_program_1, CHORDPROC_GETPREDECESSOR, v, res,
	 wrap (mkref (this), &vnode::get_predecessor_cb, n, cb, res));
}

void
vnode::get_predecessor_cb (chordID n, cbchordID_t cb, chord_noderes *res, 
		       clnt_stat err) 
{
  if (err) {
    net_address dr;
    cb (n, dr, CHORD_RPCFAILURE);
  } else if (res->status) {
    net_address dr;
    cb (n, dr, res->status);
  } else {
    cb (res->resok->node, res->resok->r, CHORD_OK);
  }
  delete res;
}

#if 0
chordID
vnode::nth_successorID (int n) 
{
  return (*successors)[n];
}
#endif /* 0 */

void
vnode::find_successor (chordID &x, cbroute_t cb)
{
  nfindsuccessor++;
  find_route (x, wrap (mkref (this), &vnode::find_successor_cb, x, cb));
}

void
vnode::find_successor_cb (chordID x, cbroute_t cb, chordID s, 
			  route search_path, chordstat status)
{
  //  warnx << "find_successor_cb: succ of " << x << " is " << s << " pathlen "
  //<< search_path.size () << "\n";
  //for (unsigned i = 0; i < search_path.size (); i++) {
  //warnx << search_path[i] << "\n";
  //}
  nhops += search_path.size ();
  if (search_path.size () > nmaxhops)
    nmaxhops = search_path.size ();
  cb (s, search_path, status);
}

void
vnode::find_route (chordID &x, cbroute_t cb) 
{
  nfindpredecessor++;
  if (myID == (*fingers)[1]) {    // is n the only node?
    route search_path;
    cb (myID, search_path, CHORD_OK);
  } else {
    chordID n = lookup_closestpred (x);
    route search_path;
    if ((n == myID) && (x == myID)) {
      print ();
      assert (0);
    }
    search_path.push_back (n);
    findpredecessor_cbstate *st = 
      New findpredecessor_cbstate (x, search_path, cb);
    testrange_findclosestpred (n, x, st);
  }
}

void
vnode::testrange_findclosestpred (chordID n, chordID x, 
				  findpredecessor_cbstate *st)
{
  // warn << "looking for closestpred of " << n << " " << x << "\n";
  ptr<chord_testandfindarg> arg = New refcounted<chord_testandfindarg> ();
  arg->v.n = n;
  arg->x = x;
  chord_testandfindres *nres = New chord_testandfindres (CHORD_OK);
  ntestrange++;
  doRPC (n, chord_program_1, CHORDPROC_TESTRANGE_FINDCLOSESTPRED, arg, nres, 
	 wrap (this, &vnode::testrange_findclosestpred_cb, nres, st));
}

void
vnode::testrange_findclosestpred_cb (chord_testandfindres *res,
				     findpredecessor_cbstate *st, 
				     clnt_stat err) 
{
  if (err) {
    warnx << "testrange_findclosestpred_cb: failure " << err << "\n";
    chordID l = st->search_path.back ();
    st->cb (l, st->search_path, CHORD_RPCFAILURE);
    delete st;
  } else if (res->status == CHORD_INRANGE) { 
    // found the successor
    locations->cacheloc (res->inres->x, res->inres->r,
			 wrap (this, &vnode::testrange_fcp_done_cb, st));
  } else if (res->status == CHORD_NOTINRANGE) {
    // haven't found the successor yet
    chordID last = st->search_path.back ();
    if (last == res->noderes->node) {   
      // last returns itself as best predecessor, but doesn't know
      // what its immediate successor is---higher layer should use
      // succlist to make forward progress
      st->cb (st->search_path.back (), st->search_path, CHORD_ERRNOENT);
      delete st;
    } else {
      // make sure that the new node sends us in the right direction,
      chordID olddist = distance (st->search_path.back (), st->x);
      chordID newdist = distance (res->noderes->node,      st->x);
      if (newdist > olddist) {
	warnx << "PROBLEM: went in the wrong direction: " << myID
	      << "looking for " << st->x << "\n";
	// xxx surely we can do something more intelligent here.
	for (unsigned i = 0; i < st->search_path.size (); i++) {
	  warnx << st->search_path[i] << "\n";
	}
	assert (0);
      }
      
      // ask the new node for its best predecessor
      locations->cacheloc (res->noderes->node, res->noderes->r,
			   wrap (this, &vnode::testrange_fcp_step_cb, st));
    }
  } else {
    warn("WTF");
    delete st;
  }
  delete res;
}

void
vnode::testrange_fcp_done_cb (findpredecessor_cbstate *st,
			      chordID s, bool ok, chordstat status)
{
  if (ok && status == CHORD_OK) {
    st->search_path.push_back (s);
    assert (st->search_path.size () < 1000);
    st->cb (st->search_path.back (), st->search_path, CHORD_OK);
    delete st;
  } else if (status == CHORD_RPCFAILURE) {
    // xxx? should we retry locally before failing all the way to
    //      the top-level?
    st->cb (st->search_path.back (), st->search_path, CHORD_RPCFAILURE);
  } else {
    warnx << myID << ": testrange_findclosest_pred: last challenge for "
	  << s << " failed. (chordstat " << status << ")\n";
    assert (0); // XXX handle malice more intelligently
  }
}

void
vnode::testrange_fcp_step_cb (findpredecessor_cbstate *st,
			      chordID s, bool ok, chordstat status)
{
  if (ok && status == CHORD_OK) {
    st->search_path.push_back (s);
    if (st->search_path.size () >= 1000) {
      warnx << "PROBLEM: too long a search path: " << myID << " looking for "
	    << st->x << "\n";
      for (unsigned i = 0; i < st->search_path.size (); i++) {
	warnx << st->search_path[i] << "\n";
      }
      assert (0);
    }
    testrange_findclosestpred (s, st->x, st);
  } else if (status == CHORD_RPCFAILURE) {
    // xxx? should we retry locally before failing all the way to
    //      the top-level?
    st->cb (st->search_path.back (), st->search_path, CHORD_RPCFAILURE);
  } else {
    warnx << myID << ": testrange_findclosest_pred: step challenge for "
	  << s << " failed. (chordstat " << status << ")\n";
    assert (0); // XXX handle malice more intelligently
  }
}
  

void
vnode::notify (chordID &n, chordID &x)
{
  ptr<chord_nodearg> na = New refcounted<chord_nodearg>;
  chordstat *res = New chordstat;
  nnotify++;
  // warnx << "notify " << n << " about " << x << "\n";
  na->v.n = n;
  na->n.x = x;
  na->n.r = locations->getaddress (x);
  doRPC (n, chord_program_1, CHORDPROC_NOTIFY, na, res, 
	 wrap (mkref (this), &vnode::notify_cb, n, res));
}

void
vnode::notify_cb (chordID n, chordstat *res, clnt_stat err)
{
  if (err || *res) {
    if (err)
      warnx << "notify_cb: RPC failure " << n << " " << err << "\n";
    else
      warnx << "notify_cb: RPC error" << n << " " << *res << "\n";
  }
  delete res;
}

void
vnode::alert (chordID &n, chordID &x)
{
  ptr<chord_nodearg> na = New refcounted<chord_nodearg>;
  chordstat *res = New chordstat;
  nalert++;
  warnx << "alert: " << x << " died; notify " << n << "\n";
  na->v.n = n;
  na->n.x = x;
  na->n.r = locations->getaddress (x);
  doRPC (n, chord_program_1, CHORDPROC_ALERT, na, res, 
		    wrap (mkref (this), &vnode::alert_cb, res));
}

void
vnode::alert_cb (chordstat *res, clnt_stat err)
{
  if (err) {
    warnx << "alert_cb: RPC failure " << err << "\n";
  } else if (*res != CHORD_UNKNOWNNODE) {
    warnx << "alert_cb: returns " << *res << "\n";
  }
  delete res;
}

void 
vnode::get_fingers (chordID &x)
{
  chord_getfingersres *res = New chord_getfingersres (CHORD_OK);
  ptr<chord_vnode> v = New refcounted<chord_vnode>;
  ngetfingers++;
  v->n = x;
  doRPC (x, chord_program_1, CHORDPROC_GETFINGERS, v, res,
	 wrap (mkref (this), &vnode::get_fingers_cb, x, res));
}

void
vnode::get_fingers_cb (chordID x, chord_getfingersres *res,  clnt_stat err) 
{
  if (err) {
    warnx << "get_fingers_cb: RPC failure " << err << "\n";
  } else if (res->status) {
    warnx << "get_fingers_cb: RPC error " << res->status << "\n";
  } else {
    for (unsigned i = 0; i < res->resok->fingers.size (); i++)
      locations->cacheloc (res->resok->fingers[i].x, 
			   res->resok->fingers[i].r,
			   wrap (this, &vnode::get_fingers_chal_cb, x));
  }
  delete res;
}

void
vnode::get_fingers_chal_cb (chordID o, chordID x, bool ok, chordstat s)
{
  // Our successors and fingers are updated automatically.
  if (s == CHORD_RPCFAILURE) {
    // maybe test for something else??
    // XXX alert o perhaps?
    warnx << myID << ": get_fingers: received bad finger from " << o << "\n";
  }
}

void
vnode::addHandler (const rpc_program &prog, cbdispatch_t cb) 
{
  dispatch_table.insert (prog.progno, cb);
  chordnode->handleProgram (prog);
};

void
vnode::doRPC (chordID &ID, rpc_program prog, int procno, 
	      ptr<void> in, void *out, aclnt_cb cb) {
  locations->doRPC (ID, prog, procno, in, out, cb);
}

