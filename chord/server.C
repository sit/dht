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

#include <chord_util.h>
#include <chord.h>
#include <route.h>

void 
vnode::get_successor (chordID n, cbchordID_t cb)
{
  //  warn << "get successor of " << n << "\n";
  ngetsuccessor++;
  chord_noderes *res = New chord_noderes (CHORD_OK);
  ptr<chordID> v = New refcounted<chordID> (n);
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
    cb (res->resok->x, res->resok->r, CHORD_OK);
  }
  delete res;
}

void
vnode::get_succlist (const chordID &n, cbchordIDlist_t cb)
{
  ngetsucclist++;
  chord_nodelistres *res = New chord_nodelistres (CHORD_OK);
  ptr<chordID> v = New refcounted<chordID> (n);
  doRPC (n, chord_program_1, CHORDPROC_GETSUCCLIST, v, res,
	 wrap (mkref (this), &vnode::get_succlist_cb, cb, res));
}

void
vnode::get_succlist_cb (cbchordIDlist_t cb, chord_nodelistres *res,
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
vnode::get_predecessor (chordID n, cbchordID_t cb)
{
  ptr<chordID> v = New refcounted<chordID> (n);
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
    cb (res->resok->x, res->resok->r, CHORD_OK);
  }
  delete res;
}

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
vnode::find_route (chordID &x, cbroute_t cb) 
{
  route_iterator *ri = factory->produce_iterator_ptr (x);
  ri->first_hop(wrap (this, &vnode::find_route_hop_cb, cb, ri));
}


void
vnode::find_route_hop_cb (cbroute_t cb, route_iterator *ri, bool done)
{
  if (done) {
    // warnx << "find_route_hop_cb " << ri->key () << " path: " 
    //  << ri->path().size() << " at node " 
    //  << ri->last_node () << "\n";
    // ri->print ();
    cb (ri->last_node (), ri->path (), ri->status ());
    delete ri;
  } else {
    // warnx << "find_route_hop_cb " << ri->key () << " hop "<<ri->last_node () 
    //  << "\n";
    // ri->print ();
    ri->next_hop ();
  }
}

void
vnode::notify (chordID &n, chordID &x)
{
  ptr<chord_nodearg> na = New refcounted<chord_nodearg>;
  chordstat *res = New chordstat;
  nnotify++;
  // warnx << "notify " << n << " about " << x << "\n";
  na->v   = n;
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
  na->v   = n;
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
  chord_nodelistres *res = New chord_nodelistres (CHORD_OK);
  ptr<chordID> v = New refcounted<chordID> (x);
  ngetfingers++;
  doRPC (x, chord_program_1, CHORDPROC_GETFINGERS, v, res,
	 wrap (mkref (this), &vnode::get_fingers_cb, x, res));
}

void
vnode::get_fingers_cb (chordID x, chord_nodelistres *res,  clnt_stat err) 
{
  if (err) {
    warnx << "get_fingers_cb: RPC failure " << err << "\n";
  } else if (res->status) {
    warnx << "get_fingers_cb: RPC error " << res->status << "\n";
  } else {
    for (unsigned i = 0; i < res->resok->nlist.size (); i++)
      locations->cacheloc (res->resok->nlist[i].x, 
			   res->resok->nlist[i].r,
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
  dispatch_record *rec = New dispatch_record (prog.progno, cb);
  dispatch_table.insert (rec);
  chordnode->handleProgram (prog);
};

bool
vnode::progHandled (int progno) 
{
  return (dispatch_table[progno] != NULL);
}

cbdispatch_t 
vnode::getHandler (unsigned long prog) {
  dispatch_record *rec = dispatch_table[prog];
  assert (rec);
  return rec->cb;
}

void
vnode::register_upcall (int progno, cbupcall_t cb)
{
  upcall_record *uc = New upcall_record (progno, cb);
  upcall_table.insert (uc);
}

void
vnode::doRPC (const chordID &ID, rpc_program prog, int procno, 
	      ptr<void> in, void *out, aclnt_cb cb) {
  locations->doRPC (ID, prog, procno, in, out, cb);
}
