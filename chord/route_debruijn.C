#include "debruijn.h"

#include <location.h>
#include <locationtable.h>
#include <misc_utils.h>

// Create an imaginary node i with as many bits from k as possible and
// such that start < i <= succ.
static chordID
firstimagin (chordID start, chordID succ, chordID k, chordID *kr, int logbase)
{
  chordID i = start + 1;

  if (start == succ) {  // XXX yuck
    *kr = k;
  } else {
    uint bs;
    chordID top;
    chordID bot;
    chordID j;
    for (bs = NBIT - logbase - 1; bs > 0; bs -= logbase) {
      assert (((NBIT - 1 - bs) % logbase) == 0);
      top = start >> (bs + 1);
      i = top << (bs + 1);
      j = (top + 1) << (bs + 1);
      bot = k >> (NBIT - bs - 1);
      i = i | bot;
      j = j | bot;
      if (betweenrightincl (start, succ, i)) {
	break;
      }
      if (betweenrightincl (start, succ, j)) {
	i = j;
	break;
      }
    }
    if (bs > 0) {
      // shift bs top bits out k
      *kr = shifttopbitout (bs + 1, k);
    } else {
      *kr = k;
    }
    // warnx  << "start: " << start << " succ " << succ << " i " << i << " k " 
    //   <<  k << " bs " << bs << " kbits " << NBIT - 1 - bs << " kr " 
    //   << *kr << " logbase " << logbase << "\n";
  }
  // warnx << "i " << i << " kr " << *kr << "\n";
  return i;
}

route_debruijn::route_debruijn (ptr<debruijn> vi, chordID xi, int l) : 
  route_iterator (vi, xi), hops (0), logbase_ (l) {};

route_debruijn::route_debruijn (ptr<debruijn> vi, chordID xi, int l,
				rpc_program uc_prog,
				int uc_procno,
				ptr<void> uc_args) : 
  route_iterator (vi, xi, uc_prog, uc_procno, uc_args), hops (0), logbase_ (l)
{
}

void
route_debruijn::print ()
{
  for (unsigned i = 0; i < search_path.size (); i++) {
    warnx << search_path[i]->id () << " " << virtual_path[i] << " " << k_path[i] 
	  << "\n";
  }
}


void
route_debruijn::first_hop (cbhop_t cbi, ptr<chordID> guess)
{
  cb = cbi;

  chordID myID = v->my_ID ();
  if (v->my_succ ()->id() == myID) {  // is myID the only node?
    done = true;
    search_path.push_back (v->my_location ());
    virtual_path.push_back (x);
    k_path.push_back (x);
  } else {
    ptr<location> l;
    if (guess) l = v->locations->lookup (*guess);
    if (!l) l = v->my_location ();
    chordID k;
    chordID r = firstimagin (myID, v->my_succ ()->id (), x, &k, logbase_);

    search_path.push_back (l);
    virtual_path.push_back (r);
    k_path.push_back (k);
  }
  next_hop (); //do it anyways
}


void
route_debruijn::send (ptr<chordID> guess)
{
  first_hop (wrap (this, &route_debruijn::send_hop_cb), guess);
}

void
route_debruijn::next_hop ()
{
  ptr<location> n = search_path.back ();
  chordID i = virtual_path.back ();
  chordID k = k_path.back ();

  make_hop (n, x, k, i);
}


void
route_debruijn::send_hop_cb (bool done)
{
  if (!done) next_hop ();
}

void
route_debruijn::make_hop (ptr<location> n, chordID &x, chordID &k, chordID &i)
{
  ptr<debruijn_arg> arg = New refcounted<debruijn_arg> ();
  arg->n = n->id ();
  arg->x = x;
  arg->i = i;
  arg->k = k;

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
    xfree (marshalled_args);
  } else
    arg->upcall_prog = 0;

  debruijn_res *nres = New debruijn_res (CHORD_OK);
  v->doRPC (n, debruijn_program_1, DEBRUIJNPROC_ROUTE, arg, nres, 
	 wrap (this, &route_debruijn::make_hop_cb, deleted, nres));
}

#if 0
static int 
uniquepathsize (vec<chordID> path) {
  int n = 1;
  for (unsigned int i = 1; i < path.size (); i++) {
    if (path[i] != path[i-1]) n++;
  }
  return n;
}
#endif /* 0 */

void
route_debruijn::make_hop_cb (ptr<bool> del, debruijn_res *res, 
			     clnt_stat err)
{
  if (*del) return;
  if (err) {
    warnx << "make_hop_cb: failure " << err << "\n";
    
    delete res;
  } else if (res->status == CHORD_STOP || last_hop) {
    r = CHORD_OK;
    cb (done = true);
  } else if (res->status == CHORD_INRANGE) { 
    // found the successor
    //BAD LOC (ok)
    ptr<location> n0 = v->locations->insert (make_chord_node (res->inres->node));
    if (!n0) {
      warnx << v->my_ID () << ": debruijn::make_hop_cb: inrange node ("
	    << res->inres->node << ") not valid vnode!\n";
      assert (0); // XXX handle malice more intelligently
    }
    search_path.push_back (n0);
    virtual_path.push_back (x);   // push some virtual path node on
    k_path.push_back (0);  // k is shifted

    successors_.clear ();
    for (size_t i = 0; i < res->inres->succs.size (); i++)
      successors_.push_back (make_chord_node (res->inres->succs[i]));

    if (stop) done = true;  // XXX necessary?
    last_hop = true;
    //    warnx << "make_hop_cb: x " << x << " path " << search_path.size() - 1
    //  << " ipath " << uniquepathsize (virtual_path) - 1 << " :\n";
    //    print ();
    cb (done);
  } else if (res->status == CHORD_NOTINRANGE) {
    // haven't found the successor yet
    ptr<location> n0 = v->locations->insert (make_chord_node (res->noderes->node));
    if (!n0) {
      warnx << v->my_ID () << ": debruijn::make_hop_cb: inrange node ("
	    << res->noderes->node << ") not valid vnode!\n";
      assert (0); // XXX handle malice more intelligently
    }
    
    chordID i = res->noderes->i;
    chordID k = res->noderes->k;
    if (i != virtual_path.back ()) hops++;
    search_path.push_back (n0);
    virtual_path.push_back (i);
    k_path.push_back (k);
      
    successors_.clear ();
    for (size_t i = 0; i < res->noderes->succs.size (); i++)
      successors_.push_back (make_chord_node (res->noderes->succs[i]));
    
    if ((hops > NBIT) || (search_path.size() > 400) ) {
      warnx << "make_hop_cb: too long a search path: " << v->my_ID() 
	    << " looking for " << x << " k = " << k << "\n";
      print ();
      assert (0);
    }
    if (stop) done = true;
    cb (done);
  } else {
    warnx << "Unexpected status: " << res->status << "\n";
    assert (0);
  }
  delete res;
}

ptr<location>
route_debruijn::pop_back ()
{
  return NULL;
}

