#include <chord.h>

/*
 *
 * Server.C
 *
 * This file implements methods of the p2p object which operate necessarily on a number of nodes
 * 
 */

void 
p2p::get_successor (sfs_ID n, cbsfsID_t cb)
{
  sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
  doRPC (n, SFSP2PPROC_GETSUCCESSOR, NULL, res,
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
  doRPC (n, SFSP2PPROC_GETPREDECESSOR, NULL, res,
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

  sfsp2p_findarg *fap = New sfsp2p_findarg;
  sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
  fap->x = x;

  route search_path;
  doRPC (n, SFSP2PPROC_FINDCLOSESTSUCC, fap, res,
	 wrap (mkref (this), &p2p::find_successor_cb, n, cb, res, search_path));
}

void
p2p::find_successor_cb (sfs_ID n, cbroute_t cb, 
		       sfsp2p_findres *res, 
		       route search_path,
		       clnt_stat err)
{
  if (err) {
    warnx << "find_closestsucc_cb: RPC failure " << err << "\n";
    deleteloc (n);
    cb (n, search_path, SFSP2P_RPCFAILURE);
  } else if (res->status) {
    warnx << "find_closestsucc_cb: RPC error" << res->status << "\n";
    cb (n, search_path, res->status);
  } else if (n != res->resok->node) {
    updateloc (res->resok->node, res->resok->r, n);
    search_path.push_back(res->resok->r);
    find_successor (res->resok->node, res->resok->x, cb);
  } else {
    updateloc (res->resok->node, res->resok->r, n);
    search_path.push_back(res->resok->r);
    cb (res->resok->node, search_path, SFSP2P_OK);
    //this is where caching will be ressurected
  }
}

void
p2p::find_predecessor (sfs_ID &n, sfs_ID &x, cbroute_t cb)
{
  sfsp2p_findarg *fap = New sfsp2p_findarg;
  sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
  fap->x = x;
  
  route search_path;
  doRPC (n, SFSP2PPROC_FINDCLOSESTPRED, fap, res,
	 wrap (mkref (this), &p2p::find_predecessor_cb, n, cb, res, search_path));
}

void
p2p::find_predecessor_cb (sfs_ID n, cbroute_t cb, 
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
  } else if (n != res->resok->node) {
    updateloc (res->resok->node, res->resok->r, n);
    search_path.push_back(res->resok->r);
    find_predecessor (res->resok->node, res->resok->x, cb);
  } else {
    updateloc (res->resok->node, res->resok->r, n);
    search_path.push_back(res->resok->r);
    cb (res->resok->node, search_path, SFSP2P_OK);
  }
}
