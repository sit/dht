#include <chord_util.h>
#include <chord.h>

/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

void 
vnode::get_successor (chordID n, cbsfsID_t cb)
{
  ngetsuccessor++;
  chord_noderes *res = New chord_noderes (CHORD_OK);
  ptr<chord_vnode> v = New refcounted<chord_vnode>;
  v->n = n;
  warnt("CHORD: issued_GET_SUCCESSOR");
  locations->doRPC (n, chord_program_1, CHORDPROC_GETSUCCESSOR, v, res,
	 wrap (mkref (this), &vnode::get_successor_cb, n, cb, res));
}

void
vnode::get_successor_cb (chordID n, cbsfsID_t cb, chord_noderes *res, 
		       clnt_stat err) 
{
  if (err) {
    net_address dr;
    warnx << "get_successor_cb: RPC failure " << err << "\n";
    chordnode->deletefingers (n);
    cb (n, dr, CHORD_RPCFAILURE);
  } else if (res->status) {
    net_address dr;
    warnx << "get_successor_cb: RPC error " << res->status << "\n";
    cb (n, dr, res->status);
  } else {
    cb (res->resok->node, res->resok->r, CHORD_OK);
  }
}

// short hand version of get_successor to avoid passing net_address around 
// if we don't need it
void
vnode::get_succ (chordID n, callback<void, chordID, chordstat>::ref cb) 
{
  get_successor (n, wrap(this, &vnode::get_succ_cb, cb));
}

void
vnode::get_succ_cb (callback<void, chordID, chordstat>::ref cb, 
		  chordID succ, net_address r, chordstat err) 
{
  cb (succ, err);
}

void 
vnode::get_predecessor (chordID n, cbsfsID_t cb)
{
  ptr<chord_vnode> v = New refcounted<chord_vnode>;
  v->n = n;
  ngetpredecessor++;
  chord_noderes *res = New chord_noderes (CHORD_OK);
  locations->doRPC (n, chord_program_1, CHORDPROC_GETPREDECESSOR, v, res,
	 wrap (mkref (this), &vnode::get_predecessor_cb, n, cb, res));
}

void
vnode::get_predecessor_cb (chordID n, cbsfsID_t cb, chord_noderes *res, 
		       clnt_stat err) 
{
  if (err) {
    net_address dr;
    warnx << "get_predecessor_cb: RPC failure " << err << "\n";
    chordnode->deletefingers (n);
    cb (n, dr, CHORD_RPCFAILURE);
  } else if (res->status) {
    net_address dr;
    warnx << "get_predecessor_cb: RPC error " << res->status << "\n";
    cb (n, dr, res->status);
  } else {
    cb (res->resok->node, res->resok->r, CHORD_OK);
  }
}

void
vnode::find_successor (chordID &n, chordID &x, cbroute_t cb)
{
  warn << "find_successor: " << n << " " << x << "\n";
  nfindsuccessor++;
  find_predecessor (n, x,
		    wrap (mkref (this), &vnode::find_predecessor_cb, cb, x));
}


void
vnode::find_successor_restart (chordID &n, chordID &x, route search_path, 
			     cbroute_t cb)
{
  // warnx << "find_successor_restart: at " << n << "\n";
  nfindsuccessorrestart++;
  find_predecessor_restart (n, x, search_path,
			    wrap (mkref (this), &vnode::find_predecessor_cb, 
				  cb, x));
}

void
vnode::find_predecessor_cb (cbroute_t cb, chordID x, chordID p, 
			    route search_path, chordstat status)
{
  nhops += search_path.size ();
  if (status == CHORD_CACHEHIT) {
    cb (p, search_path, CHORD_OK);
  }
  else if (status != CHORD_OK) {
    cb (p, search_path, status);
  } else {
    if (search_path.size () == 0) {
      // warnx << "find_predecessor_cb: get successor of " << p << "\n";
      get_successor (p, wrap (mkref(this), &vnode::find_successor_cb, 
			      cb, search_path));
    } else {
      warnx << "find_predecessor_cb: " << myID << " find " << x << "\n";
      for (unsigned i = 0; i < search_path.size (); i++) {
	warnx << search_path[i] << "\n";
      }
      chordID s = search_path.pop_back ();
      cb(s, search_path, status);
    }
  }
}

void
vnode::find_successor_cb (cbroute_t cb, route search_path, chordID s, 
			net_address r, chordstat status)
{
  // warnx << "find_successor_cb: " << s << " status " << status << "\n";
  cb (s, search_path, status);
}

void
vnode::find_predecessor (chordID &n, chordID &x, cbroute_t cb) 
{
  nfindpredecessor++;
  warnt("CHORD: find_predecessor_enter");
  if (n == finger_table[1].first.n) {
    //n is only node
    warnt("CHORD: find_pred_early_exit");
    route search_path;
    cb (n, search_path, CHORD_OK);
  } else {
    testSearchCallbacks (n, x, wrap (this, &vnode::find_pred_test_cache_cb,
				     n, x, cb));
  }
}

void
vnode::find_predecessor_restart (chordID &n, chordID &x, route search_path,
			       cbroute_t cb)
{
  ptr<chord_findarg> fap = New refcounted<chord_findarg>;
  chord_noderes *res = New chord_noderes (CHORD_OK);
  findpredecessor_cbstate *st = 
      New findpredecessor_cbstate (x, n, search_path, cb);
  nfindpredecessorrestart++;
  fap->v.n = n;
  fap->x = x;
  locations->doRPC (n, chord_program_1, CHORDPROC_FINDCLOSESTPRED, fap, res,
	 wrap (mkref (this), &vnode::find_closestpred_cb, n, st, res));
}

void
vnode::find_pred_test_cache_cb (chordID n, chordID x, cbroute_t cb, int found) 
{
  route search_path;
  search_path.push_back(n);
  if (!found) {
    ptr<chord_findarg> fap = New refcounted<chord_findarg>;
    chord_noderes *res = New chord_noderes (CHORD_OK);
    findpredecessor_cbstate *st = 
      New findpredecessor_cbstate (x, n, search_path, cb);
    fap->v.n = n;
    fap->x = x;
    warnt("CHORD: issued_FINDCLOSESTPRED_RPC");
    locations->doRPC (n, chord_program_1, CHORDPROC_FINDCLOSESTPRED, fap, res,
	   wrap (mkref (this), &vnode::find_closestpred_cb, n, st, res));
  } else {
    cb (n, search_path, CHORD_CACHEHIT);
  }
}

void
vnode::find_closestpred_cb (chordID n, findpredecessor_cbstate *st, 
			  chord_noderes *res, clnt_stat err)
{
  warnt("CHORD: find_closestpred_cb");
  if (err) {
    warnx << "find_closestpred_cb: RPC failure " << err << "\n";
    chordnode->deletefingers (n);
    st->cb (n, st->search_path, CHORD_RPCFAILURE);
  } else if (res->status) {
    warnx << "find_closestpred_cb: RPC error" << res->status << "\n";
    st->cb (n, st->search_path, res->status);
  } else {
    warnx << "find_closestpred_cb: pred of " << st->x << " is " 
	  << res->resok->node << "\n";
    locations->cacheloc (res->resok->node, res->resok->r, n);
    st->search_path.push_back(res->resok->node);
    findpredecessor_cbstate *stn = 
      New findpredecessor_cbstate (st->x, res->resok->node,
				   st->search_path, st->cb);
    testSearchCallbacks(res->resok->node, st->x,
			wrap(this, &vnode::find_closestpred_test_cache_cb,
			     res->resok->node, stn));
  }
  delete st;
}

void
vnode::find_closestpred_test_cache_cb (chordID node, 
				     findpredecessor_cbstate *st, int found) 
{ 
  if (!found) {
    ptr<chord_testandfindarg> arg = New refcounted<chord_testandfindarg> ();
    arg->v.n = node;
    arg->x = st->x;
    chord_testandfindres *res = New chord_testandfindres (CHORD_OK);
    warnt("CHORD: issued_testandfind");
    ntestrange++;
    locations->doRPC (node, chord_program_1, 
		      CHORDPROC_TESTRANGE_FINDCLOSESTPRED, arg, res,
		      wrap (this, &vnode::test_and_find_cb, res, st));

#ifdef UNOPT
    get_successor (node, 
		   wrap (mkref (this), &vnode::find_closestpred_succ_cb, st));
#endif /* UNOPT */

  } else {
    warn << "CACHE HIT\n";
    exit(10);
    st->cb (st->nprime, st->search_path, CHORD_CACHEHIT);
    //    delete st;
  }
}

void
vnode::test_and_find_cb (chord_testandfindres *res, findpredecessor_cbstate *st,
		       clnt_stat err) 
{
  warnt("CHORD: test_and_find_cb");
  if (err) {
    chordnode->deletefingers(st->nprime);
    st->cb(st->nprime, st->search_path, CHORD_RPCFAILURE);
    delete st;
    warnt("CHORD: test_and_find RPC ERROR");
  } else if (res->status == CHORD_INRANGE) { 
    // XXX returns the wrong result if the top level call is find_predecessor
    // instead of find_successor
    st->search_path.push_back(res->inres->x);
    locations->cacheloc (res->inres->x, res->inres->r, st->nprime);
    warn << "test_and_find_cb: " << myID << " succ of " << st->nprime 
	 << " is " << res->inres->x << "\n";
    st->cb (st->nprime, st->search_path, CHORD_OK);
    delete st;
  } else if (res->status == CHORD_NOTINRANGE) {
    if (st->nprime == res->noderes->node) {
      // st->nprime is the only node in the network
      st->search_path.push_back (res->noderes->node);
      st->cb (st->nprime, st->search_path, CHORD_OK);
      delete st;
    } else {
      chord_noderes *fres = New chord_noderes ();
      fres->resok->node = res->noderes->node;
      fres->resok->r = res->noderes->r;
      find_closestpred_cb (st->nprime, st, fres, RPC_SUCCESS);
      delete fres;
    }
  } else {
    warn("WTF");
    delete st;
  }
}

// XXX out of date
void
vnode::find_closestpred_succ_cb (findpredecessor_cbstate *st,
			       chordID s, net_address r, chordstat status)
{
  warnt("CHORD: find_closestpred_succ_cb");
  if (status) {
    warnx << "find_closestpred_succ_cb: failure " << status << "\n";
    st->cb (st->x, st->search_path, status);
    delete st;
  } else {
    warnx << "find_closestpred_succ_cb: " << myID << " succ of " 
	  << st->nprime << " is " << s << "\n";
    if (st->nprime == s) {
      warnx << "find_closestpred_succ_cb: " << s << " is the only Chord node\n";
      st->cb (st->nprime, st->search_path, CHORD_OK);
      delete st;
    } else if (!betweenrightincl (st->nprime, s, st->x)) {
      // XXX should we add something to the search path ???
      warnx << "find_closestpred_succ_cb: " << st->x << " is not between " 
	    << st->nprime << " and " << s << "\n";
      ptr<chord_findarg> fap = New refcounted<chord_findarg>;
      chord_noderes *res = New chord_noderes (CHORD_OK);
      fap->v.n = st->nprime;
      fap->x = st->x;
      warnt("CHORD: issued_FINDCLOSESTPRED_RPC");
      locations->doRPC (st->nprime, chord_program_1, 
			CHORDPROC_FINDCLOSESTPRED, fap, res,
			wrap (mkref (this), &vnode::find_closestpred_cb, 
			      st->nprime, st, res));
    } else {
      warnx << "find_closestpred_succ_cb: " << st->x << " is between " 
            << st->nprime << " and " << s << "\n";
      st->cb (st->nprime, st->search_path, CHORD_OK);
      delete st;
    }
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
  locations->doRPC (n, chord_program_1, CHORDPROC_NOTIFY, na, res, 
	 wrap (mkref (this), &vnode::notify_cb, n, res));
}

void
vnode::notify_cb (chordID n, chordstat *res, clnt_stat err)
{
  if (err) {
    warnx << "notify_cb: RPC failure " << n << " " << err << "\n";
    chordnode->deletefingers (n);
  } else if (*res != CHORD_OK) {
    warnx << "notify_cb: RPC error" << n << " " << *res << "\n";
  }
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
  // na->r = l->addr;
  locations->doRPC (n, chord_program_1, CHORDPROC_ALERT, na, res, 
		    wrap (mkref (this), &vnode::alert_cb, res));
}

void
vnode::alert_cb (chordstat *res, clnt_stat err)
{
  if (err) {
    warnx << "alert_cb: RPC failure " << err << "\n";
  } else if (*res != CHORD_OK) {
    warnx << "alert_cb: RPC error" << *res << "\n";
  }
}

void 
vnode::get_fingers (chordID &x)
{
  chord_getfingersres *res = New chord_getfingersres (CHORD_OK);
  ptr<chord_vnode> v = New refcounted<chord_vnode>;
  ngetfingers++;
  v->n = x;
  locations->doRPC (x, chord_program_1, CHORDPROC_GETFINGERS, v, res,
	 wrap (mkref (this), &vnode::get_fingers_cb, x, res));
}

void
vnode::get_fingers_cb (chordID x, chord_getfingersres *res,  clnt_stat err) 
{
  if (err) {
    net_address dr;
    warnx << "get_fingers_cb: RPC failure " << err << "\n";
    chordnode->deletefingers (x);
  } else if (res->status) {
    net_address dr;
    warnx << "get_fingers_cb: RPC error " << res->status << "\n";
  } else {
    warnx << "get_fingers_cb: " << res->resok->fingers.size () << " fingers\n";
    for (unsigned i = 0; i < res->resok->fingers.size (); i++) {
      warnx << "get_fingers_cb: " << res->resok->fingers[i].x << "\n";
      updatefingers (res->resok->fingers[i].x, res->resok->fingers[i].r);
    }
    print ();
  }
}



