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
  warn << "FS: " << n << " " << x << "\n";
  find_predecessor (n, x, 
		  wrap (mkref (this), &p2p::find_predecessor_cb, cb));
}

void
p2p::find_predecessor_cb (cbroute_t cb, sfs_ID p, route search_path, 
			sfsp2pstat status)
{
  if (status != SFSP2P_OK) {
    cb (p, search_path, status);
  } else {
    warnx << "find_predecessor_cb: " << p << "\n";
    get_successor (p, wrap (mkref(this), &p2p::find_successor_cb, 
				   cb, search_path));
  }
}

void
p2p::find_successor_cb (cbroute_t cb, route search_path, sfs_ID s, 
			  net_address r, sfsp2pstat status)
{
  warnx << "find_successor_cb: " << s << "\n";
  cb (s, search_path, status);
}

void
p2p::find_predecessor (sfs_ID &n, sfs_ID &x, 
		       cbroute_t cb)
{
  sfsp2p_findarg *fap = New sfsp2p_findarg;
  sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
  fap->x = x;
  
  route search_path;
  doRPC (n, sfsp2p_program_1, SFSP2PPROC_FINDCLOSESTPRED, fap, res,
	 wrap (mkref (this), &p2p::find_closestpred_cb, n, cb, res, 
	       search_path));
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
    warnx << "find_closestpred_cb: pred of " << res->resok->x << " is " 
	  << res->resok->node << "\n";
    
    updateloc (res->resok->node, res->resok->r, n);
    search_path.push_back(res->resok->node);
    if (!(between (res->resok->node, n, res->resok->x) ||
	  res->resok->node == n) ) {
      
      //      int found = testSearchCallbacks(res->resok->node, res->resok->x);

      find_successor (res->resok->node, res->resok->x, cb);
    } else {
      cb (res->resok->node, search_path, SFSP2P_OK);
    }
  }
}
