#include <chord_util.h>
#include <chord.h>

/*
 *
 * Server.C
 *
 * This file implements methods of the p2p object which operate necessarily on 
 * a number of nodes
 * 
 */

void 
p2p::get_successor (sfs_ID n, cbsfsID_t cb)
{
  sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
  warnt("CHORD: issued_GET_SUCCESSOR");
  doRPC (n, sfsp2p_program_1, SFSP2PPROC_GETSUCCESSOR, NULL, res,
	 wrap (mkref (this), &p2p::get_successor_cb, n, cb, res));
}

void
p2p::get_successor_cb (sfs_ID n, cbsfsID_t cb, sfsp2p_findres *res, 
		       clnt_stat err) 
{
  if (err) {
    net_address dr;
    warnx << "get_successor_cb: RPC failure " << err << "\n";
    deleteloc (n);
    cb (n, dr, SFSP2P_RPCFAILURE);
  } else if (res->status) {
    net_address dr;
    warnx << "get_successor_cb: RPC error " << res->status << "\n";
    cb (n, dr, res->status);
  } else {
    updateloc (res->resok->node, res->resok->r, n);
    cb (res->resok->node, res->resok->r, SFSP2P_OK);
  }
}

//short hand version of get_successor to avoid passing net_address around 
// if we don't need it
void
p2p::get_succ (sfs_ID n, callback<void, sfs_ID, sfsp2pstat>::ref cb) 
{
  get_successor (n, wrap(this, &p2p::get_succ_cb, cb));
}

void
p2p::get_succ_cb (callback<void, sfs_ID, sfsp2pstat>::ref cb, 
		  sfs_ID succ,
		  net_address r,
		  sfsp2pstat err) 
{
  cb (succ, err);
}

void 
p2p::get_predecessor (sfs_ID n, cbsfsID_t cb)
{

  sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
  doRPC (n, sfsp2p_program_1, SFSP2PPROC_GETPREDECESSOR, NULL, res,
	 wrap (mkref (this), &p2p::get_predecessor_cb, n, cb, res));
}

void
p2p::get_predecessor_cb (sfs_ID n, cbsfsID_t cb, sfsp2p_findres *res, 
		       clnt_stat err) 
{
  if (err) {
    net_address dr;
    warnx << "get_predecessor_cb: RPC failure " << err << "\n";
    deleteloc (n);
    cb (n, dr, SFSP2P_RPCFAILURE);
  } else if (res->status) {
    net_address dr;
    warnx << "get_predecessor_cb: RPC error " << res->status << "\n";
    cb (n, dr, res->status);
  } else {
    updateloc (res->resok->node, res->resok->r, n);
    cb (res->resok->node, res->resok->r, SFSP2P_OK);
  }
}

void
p2p::find_successor (sfs_ID &n, sfs_ID &x, cbroute_t cb)
{
  // warn << "find_successor: " << n << " " << x << "\n";
  find_predecessor (n, x,
		    wrap (mkref (this), &p2p::find_predecessor_cb, cb));
}


void
p2p::find_successor_restart (sfs_ID &n, sfs_ID &x, route search_path, 
			     cbroute_t cb)
{
  // warnx << "find_successor_restart: at " << n << "\n";
  find_predecessor_restart (n, x, search_path,
			    wrap (mkref (this), &p2p::find_predecessor_cb, cb));
}

void
p2p::find_predecessor_cb (cbroute_t cb, sfs_ID p, route search_path, 
			sfsp2pstat status)
{
  if (status == SFSP2P_CACHEHIT) {
    cb (p, search_path, SFSP2P_OK);
  }
  else if (status != SFSP2P_OK) {
    cb (p, search_path, status);
  } else {
    // warnx << "find_predecessor_cb: get successor of " << p << "\n";
    if (search_path.size () == 0) 
      get_successor (p, wrap (mkref(this), &p2p::find_successor_cb, 
			      cb, search_path));
    else {
      sfs_ID s = search_path.pop_back ();
      cb(s, search_path, status);
    }
  }
}

void
p2p::find_successor_cb (cbroute_t cb, route search_path, sfs_ID s, 
			net_address r, sfsp2pstat status)
{
  // warnx << "find_successor_cb: " << s << " status " << status << "\n";
  cb (s, search_path, status);
}

void
p2p::find_predecessor(sfs_ID &n, sfs_ID &x, cbroute_t cb) 
{
  warnt("CHORD: find_predecessor_enter");

  if (n == finger_table[1].first) {
    //n is only node
    warnt("CHORD: find_pred_early_exit");
    route search_path;
    cb (n, search_path, SFSP2P_OK);
  } else {
    testSearchCallbacks (n, x, wrap (this, &p2p::find_pred_test_cache_cb,
				     n, x, cb));
  }
}

void
p2p::find_predecessor_restart (sfs_ID &n, sfs_ID &x, route search_path,
			       cbroute_t cb)
{
  ptr<sfsp2p_findarg> fap = New refcounted<sfsp2p_findarg>;
  sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
  fap->x = x;
  doRPC (n, sfsp2p_program_1, SFSP2PPROC_FINDCLOSESTPRED, fap, res,
	 wrap (mkref (this), &p2p::find_closestpred_cb, n, cb, res, 
	       search_path));
}

void
p2p::find_pred_test_cache_cb (sfs_ID n, sfs_ID x, cbroute_t cb, int found) 
{
  route search_path;
  search_path.push_back(n);
  if (!found) {
    ptr<sfsp2p_findarg> fap = New refcounted<sfsp2p_findarg>;
    sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
    fap->x = x;
    warnt("CHORD: issued_FINDCLOSESTPRED_RPC");
    doRPC (n, sfsp2p_program_1, SFSP2PPROC_FINDCLOSESTPRED, fap, res,
	   wrap (mkref (this), &p2p::find_closestpred_cb, n, cb, res, 
		 search_path));
  } else {
    cb (n, search_path, SFSP2P_CACHEHIT);
  }
}

void
p2p::find_closestpred_cb (sfs_ID n, cbroute_t cb, 
			  sfsp2p_findres *res, 
			  route search_path,
			  clnt_stat err)
{
  warnt("CHORD: find_closestpred_cb");
  if (err) {
    warnx << "find_closestpred_cb: RPC failure " << err << "\n";
    deleteloc (n);
    cb (n, search_path, SFSP2P_RPCFAILURE);
  } else if (res->status) {
    warnx << "find_closestpred_cb: RPC error" << res->status << "\n";
    cb (n, search_path, res->status);
  } else {
    // warnx << "find_closestpred_cb: pred of " << res->resok->x << " is " 
    //  << res->resok->node << "\n";
    updateloc (res->resok->node, res->resok->r, n);
    
    search_path.push_back(res->resok->node);
    findpredecessor_cbstate *st = 
      New findpredecessor_cbstate (res->resok->x, res->resok->node,
				   search_path, cb);
    testSearchCallbacks(res->resok->node, res->resok->x, 
			wrap(this, &p2p::find_closestpred_test_cache_cb,
			     res->resok->node, st));
  }
}

void
p2p::find_closestpred_test_cache_cb (sfs_ID node, findpredecessor_cbstate *st, int found) 
{
  if (!found)
    {

      ptr<sfsp2p_testandfindarg> arg = New refcounted<sfsp2p_testandfindarg> ();
      arg->x = st->x;
      sfsp2p_testandfindres *res = New sfsp2p_testandfindres (SFSP2P_OK);
      warnt("CHORD: issued_testandfind");
      doRPC (node, sfsp2p_program_1, SFSP2PPROC_TESTRANGE_FINDCLOSESTPRED, arg, res,
	     wrap (this, &p2p::test_and_find_cb, res, st));

#ifdef UNOPT
      get_successor (node, 
		     wrap (mkref (this), &p2p::find_closestpred_succ_cb, st));
#endif /* UNOPT */

    }
  else 
    {
      warn << "CACHE HIT\n";
      exit(10);
      st->cb (st->nprime, st->search_path, SFSP2P_CACHEHIT);
    }
}

void
p2p::test_and_find_cb (sfsp2p_testandfindres *res, findpredecessor_cbstate *st, clnt_stat err) 
{
  warnt("CHORD: test_and_find_cb");

  if (err) {
    deleteloc(st->nprime);
    st->cb(st->nprime, st->search_path, SFSP2P_RPCFAILURE);
    warnt("CHORD: test_and_find RPC ERROR");
  } else if (res->status == SFSP2P_INRANGE) {
    st->search_path.push_back(res->inres->succ);
    updateloc (res->inres->succ, res->inres->r, st->nprime);
    //   warn << "succ of " << st->nprime << " is " << res->inres->succ << "\n";
    st->cb (st->nprime, st->search_path, SFSP2P_OK);
    delete st;
  } else if (res->status == SFSP2P_NOTINRANGE) {

    if (st->nprime == res->findres->node) {
      //st->nprime is the only node in the network
      //   st->search_path.push_back (res->findres->node);
      st->cb (st->nprime, st->search_path, SFSP2P_OK);
      delete st;
      return;
    }


    sfsp2p_findres *fres = New sfsp2p_findres ();
    fres->resok->x = res->findres->x;
    fres->resok->node = res->findres->node;
    fres->resok->r = res->findres->r;

    find_closestpred_cb (st->nprime, st->cb, fres, st->search_path, RPC_SUCCESS);
    delete fres;
    delete st;
  } else {
    warn("WTF");
  }
}

void
p2p::find_closestpred_succ_cb (findpredecessor_cbstate *st,
			       sfs_ID s, net_address r, sfsp2pstat status)
{

  warnt("CHORD: find_closestpred_succ_cb");
  if (status) {
    warnx << "find_closestpred_succ_cb: failure " << status << "\n";
    st->cb (st->x, st->search_path, status);
  } else {
    // warnx << "find_closestpred_succ_cb: succ of " 
    // << st->nprime << " is " << s << "\n";
    if (st->nprime == s) {
      warnx << "find_closestpred_succ_cb: " << s << " is the only Chord node\n";
      st->cb (st->nprime, st->search_path, SFSP2P_OK);
    } else if (!betweenrightincl (st->nprime, s, st->x)) {
      // warnx << "find_closestpred_succ_cb: " << st->x << " is not between " 
      //      << st->nprime << " and " << s << "\n";
      ptr<sfsp2p_findarg> fap = New refcounted<sfsp2p_findarg>;
      sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
      fap->x = st->x;
      warnt("CHORD: issued_FINDCLOSESTPRED_RPC");
      doRPC (st->nprime, sfsp2p_program_1, SFSP2PPROC_FINDCLOSESTPRED, fap, res,
	     wrap (mkref (this), &p2p::find_closestpred_cb, st->nprime, st->cb, 
		   res, st->search_path));
    } else {
      // warnx << "find_closestpred_succ_cb: " << st->x << " is between " 
      //       << st->nprime << " and " << s << "\n";
      st->cb (st->nprime, st->search_path, SFSP2P_OK);
    }
  }
}

void 
p2p::get_fingers (sfs_ID &x)
{
  sfsp2p_getfingersres *res = New sfsp2p_getfingersres (SFSP2P_OK);
  doRPC (x, sfsp2p_program_1, SFSP2PPROC_GETFINGERS, NULL, res,
	 wrap (mkref (this), &p2p::get_fingers_cb, x, res));
}

void
p2p::get_fingers_cb (sfs_ID x, sfsp2p_getfingersres *res,  clnt_stat err) 
{
  if (err) {
    net_address dr;
    warnx << "get_fingers_cb: RPC failure " << err << "\n";
    deleteloc (x);
  } else if (res->status) {
    net_address dr;
    warnx << "get_fingers_cb: RPC error " << res->status << "\n";
  } else {
    warnx << "get_fingers_cb: " << res->resok->fingers.size () << " fingers\n";
    for (unsigned i = 0; i < res->resok->fingers.size (); i++) {
      warnx << "get_fingers_cb: " << res->resok->fingers[i] << "\n";
      updateall (res->resok->fingers[i]);
    }
    print ();
  }
}


