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

#include "chord_util.h"
#include "chord_impl.h"
#include "route.h"
#include <transport_prot.h>
#include <coord.h>
#include <math.h>

float gforce = 1000000;

void 
vnode_impl::get_successor (const chordID &n, cbchordID_t cb)
{
  //  warn << "get successor of " << n << "\n";
  ngetsuccessor++;
  chord_noderes *res = New chord_noderes (CHORD_OK);
  ptr<chordID> v = New refcounted<chordID> (n);
  doRPC (n, chord_program_1, CHORDPROC_GETSUCCESSOR, v, res,
	 wrap (mkref (this), &vnode_impl::get_successor_cb, n, cb, res));
}

void
vnode_impl::get_successor_cb (chordID n, cbchordID_t cb, chord_noderes *res, 
			      clnt_stat err) 
{
  if (err) {
    chord_node bad;
    //    warnx << "get_successor_cb: RPC failure " << err << "\n";
    cb (bad, CHORD_RPCFAILURE);
  } else if (res->status) {
    chord_node bad;
    // warnx << "get_successor_cb: RPC error " << res->status << "\n";
    cb (bad, res->status);
  } else {
    cb (*res->resok, CHORD_OK);
  }
  delete res;
}

void
vnode_impl::get_succlist (const chordID &n, cbchordIDlist_t cb)
{
  ngetsucclist++;
  chord_nodelistres *res = New chord_nodelistres (CHORD_OK);
  ptr<chordID> v = New refcounted<chordID> (n);
  doRPC (n, chord_program_1, CHORDPROC_GETSUCCLIST, v, res,
	 wrap (mkref (this), &vnode_impl::get_succlist_cb, cb, res));
}

void
vnode_impl::get_succlist_cb (cbchordIDlist_t cb, chord_nodelistres *res,
			clnt_stat err)
{
  vec<chord_node> nlist;
  if (err) {
    cb (nlist, CHORD_RPCFAILURE);
  } else if (res->status) {
    cb (nlist, res->status);
  } else {
    // xxx there must be something more intelligent to do here
    for (unsigned int i = 0; i < res->resok->nlist.size (); i++)
      nlist.push_back (res->resok->nlist[i]);
    cb (nlist, CHORD_OK);
  }
  delete res;
}

void 
vnode_impl::get_predecessor (const chordID &n, cbchordID_t cb)
{
  ptr<chordID> v = New refcounted<chordID> (n);
  ngetpredecessor++;
  chord_noderes *res = New chord_noderes (CHORD_OK);
  doRPC (n, chord_program_1, CHORDPROC_GETPREDECESSOR, v, res,
	 wrap (mkref (this), &vnode_impl::get_predecessor_cb, n, cb, res));
}

void
vnode_impl::get_predecessor_cb (chordID n, cbchordID_t cb, chord_noderes *res, 
		       clnt_stat err) 
{
  if (err) {
    chord_node bad;
    cb (bad, CHORD_RPCFAILURE);
  } else if (res->status) {
    chord_node bad;
    cb (bad, res->status);
  } else {
    cb (*res->resok, CHORD_OK);
  }
  delete res;
}

void
vnode_impl::find_successor (const chordID &x, cbroute_t cb)
{
  nfindsuccessor++;
  find_route (x, wrap (mkref (this), &vnode_impl::find_successor_cb, x, cb));
}

void
vnode_impl::find_successor_cb (chordID x, cbroute_t cb, chordID s, 
			  route search_path, chordstat status)
{
  if (status != CHORD_OK) {
    warnx << "find_successor_cb: find successor of " 
	  << x << " failed: " << status << "\n";
  } else {
    nhops += search_path.size ();
    if (search_path.size () > nmaxhops)
      nmaxhops = search_path.size ();
  }
  cb (s, search_path, status);
}

void
vnode_impl::find_route (const chordID &x, cbroute_t cb) 
{
  route_iterator *ri = factory->produce_iterator_ptr (x);
  ri->first_hop(wrap (this, &vnode_impl::find_route_hop_cb, cb, ri));
}


void
vnode_impl::find_route_hop_cb (cbroute_t cb, route_iterator *ri, bool done)
{
  if (done) {
    cb (ri->last_node (), ri->path (), ri->status ());
    delete ri;
  } else {
    ri->next_hop ();
  }
}

void
vnode_impl::notify (const chordID &n, chordID &x)
{
  ptr<chord_nodearg> na = New refcounted<chord_nodearg>;
  chordstat *res = New chordstat;
  nnotify++;
  // warnx << gettime () << ": notify " << n << " about " << x << "\n";
  bool ok = locations->get_node (x, &na->n);
  assert (ok);
  
  doRPC (n, chord_program_1, CHORDPROC_NOTIFY, na, res, 
	 wrap (mkref (this), &vnode_impl::notify_cb, n, res));
}

void
vnode_impl::notify_cb (chordID n, chordstat *res, clnt_stat err)
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
vnode_impl::alert (const chordID &n, chordID &x)
{
  ptr<chord_nodearg> na = New refcounted<chord_nodearg>;
  chordstat *res = New chordstat;
  nalert++;
  warnx << "alert: " << x << " died; notify " << n << "\n";
  bool ok = locations->get_node (x, &na->n);
  assert (ok);

  doRPC (n, chord_program_1, CHORDPROC_ALERT, na, res, 
		    wrap (mkref (this), &vnode_impl::alert_cb, res));
}

void
vnode_impl::alert_cb (chordstat *res, clnt_stat err)
{
  if (err) {
    warnx << "alert_cb: RPC failure " << err << "\n";
  } else if (*res != CHORD_UNKNOWNNODE) {
    warnx << "alert_cb: returns " << *res << "\n";
  }
  delete res;
}

void 
vnode_impl::get_fingers (const chordID &x, cbchordIDlist_t cb)
{
  chord_nodelistres *res = New chord_nodelistres (CHORD_OK);
  ptr<chordID> v = New refcounted<chordID> (x);
  ngetfingers++;
  doRPC (x, chord_program_1, CHORDPROC_GETFINGERS, v, res,
	 wrap (mkref (this), &vnode_impl::get_fingers_cb, cb, x, res));
}

void
vnode_impl::get_fingers_cb (cbchordIDlist_t cb,
			    chordID x, chord_nodelistres *res, clnt_stat err) 
{
  vec<chord_node> nlist;
  if (err) {
    warnx << "get_fingers_cb: RPC failure " << err << "\n";
    cb (nlist, CHORD_RPCFAILURE);
  } else if (res->status) {
    warnx << "get_fingers_cb: RPC error " << res->status << "\n";
    cb (nlist, res->status);
  } else {
    // xxx there must be something more intelligent to do here
    for (unsigned int i = 0; i < res->resok->nlist.size (); i++)
      nlist.push_back (res->resok->nlist[i]);
    cb (nlist, CHORD_OK);
  }
  delete res;
}

void
vnode_impl::addHandler (const rpc_program &prog, cbdispatch_t cb) 
{
  dispatch_record *rec = New dispatch_record (prog.progno, cb);
  dispatch_table.insert (rec);
  chordnode->handleProgram (prog);
};

bool
vnode_impl::progHandled (int progno) 
{
  return (dispatch_table[progno] != NULL);
}

cbdispatch_t 
vnode_impl::getHandler (unsigned long prog) {
  dispatch_record *rec = dispatch_table[prog];
  assert (rec);
  return rec->cb;
}

void
vnode_impl::register_upcall (int progno, cbupcall_t cb)
{
  upcall_record *uc = New upcall_record (progno, cb);
  upcall_table.insert (uc);

}

void 
vnode_impl::fill_user_args (user_args *a)
{
  a->myID = myID;
  a->myindex = myindex;
  a->coords = locations->get_coords (myID);
}

void
user_args::reply (void *res)
{
  //marshall result
  xdrproc_t inproc = prog->tbl[procno].xdr_res;
  xdrsuio x (XDR_ENCODE);
  if ((!inproc) || (!inproc (x.xdrp (), res))) 
    fatal << "couldn't marshall result\n";
  int res_len = x.uio ()->resid ();
  void *marshalled_res = suio_flatten (x.uio ());

#ifdef RPC_PROGRAM_STATS
  prog->outreply_num[procno] += 1;
  prog->outreply_bytes[procno] += res_len;
#endif

  //stuff into a transport wrapper
  dorpc_res *rpc_res = New dorpc_res (DORPC_OK);

  rpc_res->resok->src_id = myID;
  rpc_res->resok->send_time_echo = send_time;
  rpc_res->resok->src_vnode_num = myindex;
  rpc_res->resok->src_coords.setsize (coords.size ());
  for (unsigned int i = 0; i < coords.size (); i++)
    rpc_res->resok->src_coords[i] = (int)(coords[i]);


  rpc_res->resok->progno = prog->progno;
  rpc_res->resok->procno = procno;
  rpc_res->resok->results.setsize (res_len);
  memcpy (rpc_res->resok->results.base (), marshalled_res, res_len);
  free (marshalled_res);

  assert (rpc_res->status == DORPC_OK);

  //reply
  sbp->reply (rpc_res);
  delete rpc_res;
  delete this;
}

void
vnode_impl::ping (const chordID &x, cbping_t cb)
{
  //close enough
  get_successor (x, wrap (this, &vnode_impl::ping_cb, cb));
}

void
vnode_impl::ping_cb (cbping_t cb, chord_node n, chordstat status) 
{
  cb (status);
}

long
vnode_impl::doRPC (const chordID &ID, const rpc_program &prog, int procno, 
	      ptr<void> in, void *out, aclnt_cb cb) {

  //form the transport RPC
  ptr<dorpc_arg> arg = New refcounted<dorpc_arg> ();

  //header
  arg->dest_id = ID;
  arg->src_id = myID;
  arg->src_vnode_num = myindex;
  arg->progno = prog.progno;
  arg->procno = procno;
  
  //marshall the args ourself
  xdrproc_t inproc = prog.tbl[procno].xdr_arg;
  xdrsuio x (XDR_ENCODE);
  if ((!inproc) || (!inproc (x.xdrp (), in))) {
    fatal << "failed to marshall args\n";
    cb (RPC_CANTSEND);
    return 0;
  } else {
    int args_len = x.uio ()->resid ();
    arg->args.setsize (args_len);
    void *marshalled_args = suio_flatten (x.uio ());
    memcpy (arg->args.base (), marshalled_args, args_len);
    free (marshalled_args);

#ifdef RPC_PROGRAM_STATS
    prog.outcall_num[procno] += 1;
    prog.outcall_bytes[procno] += args_len;
#endif

    ref<dorpc_res> res = New refcounted<dorpc_res> (DORPC_OK);
    return locations->doRPC (myID, ID, transport_program_1, TRANSPORTPROC_DORPC, 
			     arg, res, 
			     wrap (this, &vnode_impl::doRPC_cb, 
				   prog, procno, out, cb, res));
  }
}


void
vnode_impl::doRPC_cb (const rpc_program prog, int procno,
		      void *out, aclnt_cb cb, 
		      ref<dorpc_res> res, clnt_stat err) 
{
  if (err) {
    cb (err);
  } else if (res->status != DORPC_OK) {
    cb (RPC_CANTRECV);
  } else {
    
    float distance = locations->get_a_lat (res->resok->src_id);
    vec<float> u_coords;
    for (unsigned int i = 0; i < res->resok->src_coords.size (); i++) {
      float c = ((float)res->resok->src_coords[i]);
      u_coords.push_back (c);
    }

    update_coords (res->resok->src_id, 
		   u_coords,
		   distance);
    
    //unmarshall the result and copy to out
    xdrmem x ((char *)res->resok->results.base (), 
	      res->resok->results.size (), XDR_DECODE);
    xdrproc_t proc = prog.tbl[procno].xdr_res;
    assert (proc);
    if (!proc (x.xdrp (), out)) {
      fatal << "failed to unmarshall result\n";
      cb (RPC_CANTSEND);
    } else 
      cb (err);
  }
}

#define MAXDIM 10
#define DT 0.01
void
vnode_impl::update_coords (chordID u, vec<float> uc, float ud)
{


  //  warn << myID << " --- starting update -----\n";
  //update the node's coords in the locatoin table
  locations->set_coords (u, uc);
  int iterations = 0;
  float ftot = 0.0;
  vec<float> coords = locations->get_coords (myID);
  vec<float> f;

  do {
    //figure out the force on us by looking at all of the springs
    //in the location table

    //init f
    f.clear ();
    for (int i = 0; i < chord::NCOORDS; i++)
      f.push_back (0.0);

    ptr<location> l = locations->first_loc ();
    bool found_meas = false;
    int cit = 0;

    while (l) {
      if ((l->coords.size () > 0) && (l->n != myID)) {

	//  warn << myID << " setting a spring to " << l->n << "\n";
	// print_vector ("l->coords", l->coords);

	float actual = l->distance;
	float expect = Coord::distance_f (coords, l->coords);


	if (actual >= 0)
	  {
	    //force magnitude: > 0 --> stretched
	    float grad = expect - actual;
	    
	    vec<float> v = l->coords;
	    Coord::vector_sub (v, coords);
	
	    float len = Coord::norm (v);
	    float unit = 1.0/sqrtf(len);
	    
	    //scalar_mult(v, unit) is unit force vector
	    // times grad gives the scaled force vector
	    Coord::scalar_mult (v, unit*grad);
	    
	    //add v into the overall force vector
	    Coord::vector_add (f, v);
	    
	    //	Coord::print_vector ("f ", f);
	    found_meas = true;
	    cit++;
	  }
      } 
      l = locations->next_loc(l->n);
    }
    
    //print_vector ("f", f);
    if (!found_meas) {
      warn << "no springs!\n";
      return;
    } 
      
    //run the simulation for a bit
    ftot = 0.0;
    for (unsigned int k = 0; k < f.size (); k++)
      ftot += fabs (f[k]);

    float t = DT;
    while (ftot*t > 1000.0) t /= 2.0;

    gforce = ftot;

    Coord::scalar_mult(f, t);
    Coord::vector_add (coords, f);

    //    Coord::print_vector ("f ", f);
    //    Coord::print_vector("coords ", coords);
    
    iterations++;
  } while (false);

  locations->set_coords (myID, coords);
}

long
vnode_impl::doRPC (const chord_node &n, const rpc_program &prog, int procno, 
	      ptr<void> in, void *out, aclnt_cb cb) {

  //BAD LOC (ok)
  locations->insert (n);
  return doRPC (n.x, prog, procno, in, out, cb);
}

void
vnode_impl::resendRPC (long seqno)
{
  locations->resendRPC (seqno);
}
