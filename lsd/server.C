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
  //  warn << "FS: " << n << " " << x << "\n";
  
  sfs_ID start;
  if (lsd_location_lookup) {
    start = query_location_table (x);
    if (start < 0) start = n;
    warn << "starting search for " << x << " at " << start << "rather than at " << n << "\n";
  } else 
    start = n;
 
  find_predecessor (start, x,
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
    //    warnx << "find_predecessor_cb: " << p << "\n";
    get_successor (p, wrap (mkref(this), &p2p::find_successor_cb, 
				   cb, search_path));
  }
}

void
p2p::find_successor_cb (cbroute_t cb, route search_path, sfs_ID s, 
			net_address r, sfsp2pstat status)
{
  //  warnx << "find_successor_cb: " << s << "\n";
  cb (s, search_path, status);
}

void
p2p::find_predecessor(sfs_ID &n, sfs_ID &x, cbroute_t cb) 
{
  route search_path;
  if (n == finger_table[1].first) {
    cb (n, search_path, SFSP2P_OK);
  } else {
    testSearchCallbacks (n, x, wrap (this, &p2p::find_pred_test_cache_cb,
				     n, x, cb));
  }
}

void
p2p::find_pred_test_cache_cb (sfs_ID n, sfs_ID x, cbroute_t cb, int found) 
{
  route search_path;
  search_path.push_back(n);
  if (!found) {
    sfsp2p_findarg *fap = New sfsp2p_findarg;
    sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
    fap->x = x;
    doRPC (n, sfsp2p_program_1, SFSP2PPROC_FINDCLOSESTPRED, fap, res,
	   wrap (mkref (this), &p2p::find_closestpred_cb, n, cb, res, 
		 search_path));
  } else {
    //    warn << "CACHE HIT (local node)\n";
    cb (n, search_path, SFSP2P_CACHEHIT);
  }
}

void
p2p::find_closestpred_cb (sfs_ID n, cbroute_t cb, 
			  sfsp2p_findres *res, 
			  route search_path,
			  clnt_stat err)
{
  if (err) {
    warnx << "find_closestpred_cb: RPC failure " << err << "\n";
    deleteloc (n);
    cb (n, search_path, SFSP2P_RPCFAILURE);
  } else if (res->status) {
    warnx << "find_closestpred_cb: RPC error" << res->status << "\n";
    cb (n, search_path, res->status);
  } else {
    //    warnx << "find_closestpred_cb: pred of " << res->resok->x << " is " 
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
    get_successor (node, 
		   wrap (mkref (this), &p2p::find_closestpred_succ_cb, st));
    }
  else 
    {
      warn << "CACHE HIT\n";
      st->cb (st->nprime, st->search_path, SFSP2P_CACHEHIT);
    }
}


void
p2p::find_closestpred_succ_cb (findpredecessor_cbstate *st,
			       sfs_ID s, net_address r, sfsp2pstat status)
{
  if (status) {
    warnx << "find_closestpred_succ_cb: failure " << status << "\n";
    st->cb (st->x, st->search_path, status);
  } else {
    //    warnx << "find_closestpred_succ_cb: succ of " << st->nprime << " is " << s 
    //	  << "\n";
    if (st->nprime == s) {
      //      warnx << "find_closestpred_succ_cb: " << s << " is the only Chord node\n";
      st->cb (st->nprime, st->search_path, SFSP2P_OK);
    } else if (!between (st->nprime, s, st->x)) {
      //      warnx << "find_closestpred_succ_cb: " << st->x << " is not between " 
      //    << st->nprime << " and " << s << "\n";
      sfsp2p_findarg *fap = New sfsp2p_findarg;
      sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
      fap->x = st->x;
      doRPC (st->nprime, sfsp2p_program_1, SFSP2PPROC_FINDCLOSESTPRED, fap, res,
	     wrap (mkref (this), &p2p::find_closestpred_cb, st->nprime, st->cb, 
		   res, st->search_path));
    } else {
      //      warnx << "find_closestpred_succ_cb: " << st->x << " is between " 
      //	    << st->nprime << " and " << s << "\n";
      st->cb (st->nprime, st->search_path, SFSP2P_OK);
    }
  }
}

sfs_ID
p2p::query_location_table (sfs_ID x) {
  location *l = locations.first ();
  sfs_ID min = bigint(1) << 160;
  sfs_ID ret = -1;
  while (l) {
    sfs_ID d = diff(l->n, x);
    if (d < min) { min = d; ret = l->n; }
    l = locations.next (l);
  }
  return ret;
}
