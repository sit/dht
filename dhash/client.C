/*
 *
 * Copyright (C) 2001  Frank Dabek (fdabek@lcs.mit.edu), 
 *                     Frans Kaashoek (kaashoek@lcs.mit.edu),
 *   		       Massachusetts Institute of Technology
 * 
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

#include <sys/time.h>
#include <stdlib.h>

#include <sfsmisc.h>
#include <arpc.h>
#include <crypt.h>

#include <chord_types.h>
#include <chord.h>
#include <route.h>
#include <configurator.h>
#include <location.h>
#include <locationtable.h>
#include <merkle_misc.h>

#include "dhash_common.h"
#include "dhash.h"
#include "dhashcli.h"
#include "verify.h"
#include "download.h"
#include "dhash_store.h"

#include <coord.h>
#include <misc_utils.h>
#include <modlogger.h>

// HACK global indicator variable for whether or not to
//      transfer data over TCP pipes.
bool dhash_tcp_transfers = false;

#define warning modlogger ("dhashcli", modlogger::WARNING)
#define info  modlogger ("dhashcli", modlogger::INFO)
#define trace modlogger ("dhashcli", modlogger::TRACE)
int DHC = getenv("DHC") ? atoi(getenv("DHC")) : 0;

#ifdef DMALLOC
#include <dmalloc.h>
#endif
#include <ida.h>

#include "succopt.h"

static struct dhashcli_config_init {
  dhashcli_config_init ();
} dcci;

dhashcli_config_init::dhashcli_config_init ()
{
  bool ok = true;

#define set_int Configurator::only ().set_int

  /** Whether or not to order successors by expected latency */
  ok = ok && set_int ("dhashcli.order_successors", 1);

  assert (ok);
#undef set_int
}


// ---------------------------------------------------------------------------
// DHASHCLI

dhashcli::dhashcli (ptr<vnode> node)
  : clntnode (node), ordersucc_ (true)
{
  int ordersucc = 1;
  Configurator::only ().get_int ("dhashcli.order_successors", ordersucc);
  ordersucc_ = (ordersucc > 0);
  warn << "will order successors " << ordersucc_ << "\n";

}

void
dhashcli::dofetchrec_execute (blockID b, cb_ret cb)
{
  chordID myID = clntnode->my_ID ();

  ptr<dhash_fetchrec_arg> arg = New refcounted<dhash_fetchrec_arg> ();
  arg->key    = b.ID;
  arg->ctype  = b.ctype;
  arg->dbtype = b.dbtype;

  vec<chordID> failed;
  ptr<location> s = clntnode->closestpred (b.ID, failed);
  trace << myID << ": dofetchrec_execute (" << b << ") -> " << s->id () << "\n";
  
  ptr<dhash_fetchrec_res> res = New refcounted<dhash_fetchrec_res> (DHASH_OK);
  timespec start;
  clock_gettime (CLOCK_REALTIME, &start);
  clntnode->doRPC (s, dhash_program_1, DHASHPROC_FETCHREC,
		   arg, res,
		   wrap (this, &dhashcli::dofetchrec_cb, start, b, cb, res));
}

void
dhashcli::dofetchrec_cb (timespec start, blockID b, cb_ret cb,
			 ptr<dhash_fetchrec_res> res, clnt_stat s)
{
  strbuf prefix;
  prefix << clntnode->my_ID () << ": dofetchrec_execute (" << b << "): ";
  trace << prefix << "returned " << s << "\n";
  if (s) {
    route r;
    (*cb) (DHASH_RPCERR, NULL, r);
    return;
  }
  
  route r;
  rpc_vec<chord_node_wire, RPC_INFINITY> *path = (res->status == DHASH_OK) ?
    &res->resok->path :
    &res->resdef->path;
  for (size_t i = 0; i < path->size (); i++) {
    ptr<location> l = clntnode->locations->lookup_or_create
      (make_chord_node ((*path)[i]));
    assert (l != NULL);
    r.push_back (l);
  }
  
  if (res->status != DHASH_OK) {
    trace << prefix << "returned " << res->status << "\n";
    // XXX perhaps one should do something cleverer here.
      (*cb) (res->status, NULL, r);
    return;
  }

  // Okay, we have the block.  Massage it into a dhash_block

  ptr<dhash_block> blk = New refcounted<dhash_block> (res->resok->res.base (),
						      res->resok->res.size (),
						      b.ctype);
  blk->ID = b.ID;
  blk->hops = res->resok->path.size ();
  blk->errors = 0;  // XXX
  blk->retries = 0; // XXX

  // We can't directly measure the time it took for a lookup.
  // However, the remote node does report back to us the amount of time
  // it took to reconstruct the block.  Since there are only two phases,
  // we can use this time to construct the actual time required.
  timespec finish;
  clock_gettime (CLOCK_REALTIME, &finish);
  timespec total = finish - start;
  blk->times.push_back (total.tv_sec * 1000 + int (total.tv_nsec/1000000));

  u_int32_t othertimes = 0;
  for (size_t i = 0; i < res->resok->times.size (); i++) {
    blk->times.push_back (res->resok->times[i]);
    othertimes += res->resok->times[i];
  }
  blk->times[0] -= othertimes;

  (*cb) (DHASH_OK, blk, r);
  return;
}

void
dhashcli::retrieve (blockID blockID, cb_ret cb, int options, 
		    ptr<chordID> guess)
{
  chordID myID = clntnode->my_ID ();

  trace << myID << ": retrieve (" << blockID << "): new retrieve.\n";
  
  // First check to see if we're using TCP.  In that case, we should
  // ship the data over the wire.
  if (dhash_tcp_transfers) {
    dofetchrec_execute (blockID, cb);
    return;
  }
  
  ptr<rcv_state> rs = New refcounted<rcv_state> (blockID, cb);
  
  if (blockID.ctype == DHASH_KEYHASH || blockID.ctype == DHASH_NOAUTH) {
      route_iterator *ci = clntnode->produce_iterator_ptr (blockID.ID);
      ci->first_hop (wrap (this, &dhashcli::retrieve_block_hop_cb, rs, ci,
			   options, 5, guess),
		     guess);
  } else {
    
    // Optimal number of successors to fetch is the number of
    // extant fragments.  This ensures the maximal amount of choice
    // for the expensive fetch phase.  As long as proximity routing
    // is in use, the last few hops of the routing phase are cheap.
    // Unfortunately, it's hard to know if we are proximity routing.
    clntnode->find_succlist (blockID.ID,
			     dhash::num_efrags (),
			     wrap (this, &dhashcli::retrieve_lookup_cb, rs),
			     guess);
  }
}

void
dhashcli::retrieve_block_hop_cb (ptr<rcv_state> rs, route_iterator *ci,
				 int options, int retries, ptr<chordID> guess,
				 bool done)
{
  if (!done) {
    ci->next_hop ();
    return;
  }

  chordID myID = clntnode->my_ID ();

  rs->timemark ();
  rs->r = ci->path ();
  rs->succs = ci->successors ();
  dhash_stat status = ci->status () ? DHASH_CHORDERR : DHASH_OK;
  delete ci;
  
  if (status != DHASH_OK) {
    trace << myID << ": retrieve (" << rs->key
          << "): lookup failure: " << status << "\n";
    rs->complete (status, NULL); // failure
    rs = NULL;    
    return;
  }

  chord_node s = rs->succs.pop_front ();
  if (DHC && 
      (rs->key.ctype == DHASH_KEYHASH || rs->key.ctype == DHASH_NOAUTH)) {
    ptr<dhc_get_arg> arg = New refcounted<dhc_get_arg>;
    arg->bID = rs->key.ID;
    rs->ds.status = status;
    rs->ds.options = options;
    rs->ds.retries = retries;
    rs->ds.guess = guess;    
    rs->ds.res = New refcounted<dhc_get_res> (DHC_OK);

    ptr<location> l = clntnode->locations->lookup_or_create (s);
    
    warn << l->id () << " is succ of key " << arg->bID << "\n";

    clntnode->doRPC (l, dhc_program_1, DHCPROC_GET, arg, rs->ds.res,
		     wrap (this, &dhashcli::retrieve_dhc_cb, rs));
  } else 
    dhash_download::execute (clntnode, s, rs->key, NULL, 0, 0, 0,
			     wrap (this, &dhashcli::retrieve_dl_or_walk_cb,
				   rs, status, options, retries, guess));
}

void 
dhashcli::retrieve_dhc_lookup_cb (ptr<rcv_state> rs, vec<chord_node> succs, 
				  route path, chordstat err)
{
  if (!err) {
    ptr<location> l = clntnode->locations->lookup_or_create (succs[0]);
    warn << "\n\n********** succs[0] = " << succs[0].x << "\n\n\n";
    ptr<dhc_get_arg> arg = New refcounted<dhc_get_arg>;
    arg->bID = rs->key.ID;
    rs->ds.res = New refcounted<dhc_get_res> (DHC_OK);   

    clntnode->doRPC (l, dhc_program_1, DHCPROC_GET, arg, rs->ds.res,
		     wrap (this, &dhashcli::retrieve_dhc_cb, rs));
  }
}

void 
dhashcli::retrieve_dhc_cb (ptr<rcv_state> rs, clnt_stat err)
{
  if (rs->ds.res->status == DHC_OK) {
    //warn << "\n\n Retrieve success\n\n";
    ptr<dhash_block> blk = 
      New refcounted<dhash_block> (rs->ds.res->resok->data.data.base (), 
				   rs->ds.res->resok->data.data.size (),
				   rs->key.ctype);
    blk->ID = rs->key.ID;
    blk->source = clntnode->my_ID ();
    blk->hops   = 0;
    blk->errors = 0;
    
    retrieve_dl_or_walk_cb (rs, rs->ds.status, rs->ds.options, rs->ds.retries, 
			    rs->ds.guess, blk);
  } else {
    warn << "dhashcli: DHC retrieve failed dhc_stat " << rs->ds.res->status << "\n";
    retrieve_dl_or_walk_cb (rs, rs->ds.status, rs->ds.options, rs->ds.retries, 
			    rs->ds.guess, 0);
  }
}


void
dhashcli::retrieve_dl_or_walk_cb (ptr<rcv_state> rs, dhash_stat status,
				  int options, int retries, 
				  ptr<chordID> guess, ptr<dhash_block> blk)
{
  chordID myID = clntnode->my_ID ();

  if(!blk) {
    if (retries == 0 || (options & DHASHCLIENT_NO_RETRY_ON_LOOKUP)) {
      rs->complete (DHASH_NOENT, NULL);
      rs = NULL;
    }
    else if (rs->succs.size() == 0) {
      trace << myID << ": walk ("<< rs->key
	    << "): No luck walking successors, retrying..\n";
      route_iterator *ci = clntnode->produce_iterator_ptr (rs->key.ID);
      delaycb (5, wrap (ci, &route_iterator::first_hop, 
			wrap (this, &dhashcli::retrieve_block_hop_cb,
			      rs, ci, options, retries - 1, guess),
			guess));
    }
    else {
      chord_node s = rs->succs.pop_front ();
      dhash_download::execute (clntnode, s, rs->key, NULL, 0, 0, 0,
			       wrap (this, &dhashcli::retrieve_dl_or_walk_cb,
				     rs, status, options, retries,
				     guess));
    }
  } else {
    rs->timemark ();

    blk->ID = rs->key.ID;
    blk->hops = rs->r.size ();
    blk->errors = rs->nextsucc - dhash::num_dfrags ();
    blk->retries = blk->errors;

    rs->complete (DHASH_OK, blk);
    rs = NULL;
  }
}

void
dhashcli::fetch_frag (ptr<rcv_state> rs)
{
  register size_t i = rs->nextsucc;

  chordID myID = clntnode->my_ID ();

  // Ugh. No more successors available.
  if (i >= rs->succs.size ()) {
    // If there are outstanding fragments, there is still hope.
    // XXX Actually, this is a lie. If we know that they will not
    //     provide us with enough total fragments to reconstruct the
    //     block,  e.g. incoming_rpcs + frags.size < NUM_DFRAGS,
    //     we should just give up to the user now.  However for
    //     book keeping purposes, we don't do this.
    if (rs->incoming_rpcs > 0)
      return;
    
    // Should we try harder? Like, try and get more successors and
    // check out the swath? No, let's just fail and have the higher
    // level know that they should retry.
    trace << myID << ": retrieve (" << rs->key 
          << "): out of successors; failing.\n";
    rs->complete (DHASH_NOENT, NULL);
    rs = NULL;
    return;
  }
  
  rs->incoming_rpcs += 1;

  dhash_download::execute (clntnode, rs->succs[i], 
			   blockID(rs->key.ID, rs->key.ctype, DHASH_FRAG),
			   (char *)NULL, 0, 0, 0, 
			   wrap (this, &dhashcli::retrieve_fetch_cb, rs, i),
			   wrap (this, &dhashcli::on_timeout, rs));
  rs->nextsucc += 1;
}

void
dhashcli::on_timeout (ptr<rcv_state> rs, 
		      chord_node dest,
		      int retry_num) 
{
  //  if (retry_num == 1)
  //    fetch_frag (rs);
}

struct orderer {
  float d_;
  size_t i_;
  static int cmp (const void *a_, const void *b_) {
    const orderer *a = (orderer *) a_, *b = (orderer *) b_;
    return (int) (a->d_ - b->d_);
  }
};

static void
order_succs (ptr<locationtable> locations,
	     const Coord &me, const vec<chord_node> &succs,
	     vec<chord_node> &out, u_long max = 0)
{

  //max is the number of successors we should order: the first max in the list
  u_long lim = max;
  // 0 means order them all
  if (max == 0 || max > succs.size ()) lim = succs.size ();

  orderer *d2me = New orderer [succs.size()];
  for (size_t i = 0; i < lim; i++) {
    ptr<location> l = NULL;
    if (locations) 
      l = locations->lookup (succs[i].x);
    if (l) {
      // Have actual measured latencies, so might as well use them.
      d2me[i].d_ = l->distance ();
    } else {
      Coord cursucc (succs[i]);
      d2me[i].d_ = Coord::distance_f (me, cursucc);
    }
    d2me[i].i_ = i;
  }
  qsort (d2me, lim, sizeof (*d2me), &orderer::cmp);
  out.clear ();
  for (size_t i = 0; i < lim; i++) {
#ifdef VERBOSE_LOG
    char buf[10]; // argh. please shoot me.
    sprintf (buf, "%5.2f", d2me[i].d_);
    modlogger ("orderer", modlogger::TRACE) << d2me[i].i_ << " "
					    << succs[d2me[i].i_] << " "
					    << buf << "\n";
#endif /* VERBOSE_LOG */    
    out.push_back (succs[d2me[i].i_]);
  }
  delete[] d2me;

  //copy any of the ones we didn't consider verbatim
  if (max == 0) return;
  long remaining = succs.size () - lim;
  if (remaining > 0) 
    for (int i = 0; i < remaining; i++)
      out.push_back (succs[lim+i]);

}

static void
merge_succ_list (vec<chord_node> &succs,
                 const vec<ptr<location> > &from, unsigned needed)
{
  for (unsigned i=0; i<from.size () && succs.size ()<needed; i++) {
    if (from [i]->alive ()) {
      chord_node x;
      from [i]->fill_node (x);
      bool found = false;
      for (unsigned j=0; j<succs.size () && !found; j++)
        if (succs [j].x == x.x)
	  found = true;
      if (!found)
        succs.push_back (x);
    }
  }
}


void
dhashcli::retrieve_lookup_cb (ptr<rcv_state> rs, vec<chord_node> succs,
			      route r, chordstat status)
{
  chordID myID = clntnode->my_ID ();
  rs->timemark ();
  rs->r = r;
  
  if (status) {
    trace << myID << ": retrieve (" << rs->key 
          << "): lookup failure: " << status << "\n";
    rs->complete (DHASH_CHORDERR, NULL); // failure
    rs = NULL;
    return;
  }
  strbuf s;
  s << myID << ": retrieve_verbose (" << rs->key << "): route";
  for (size_t i = 0; i < r.size (); i++)
    s << " " << r[i]->id ();
  s << "\n";
  trace << s;
  s.tosuio ()->clear ();
  s << myID << ": retrieve_verbose (" << rs->key << "): succs";
  for (size_t i = 0; i < succs.size (); i++)
    s << " " << succs[i].x;
  s << "\n";
  trace << s;

  doassemble (rs, succs);
}

void
dhashcli::retrieve_fetch_cb (ptr<rcv_state> rs, u_int i,
			     ptr<dhash_block> block)
{
  chordID myID = clntnode->my_ID ();

  rs->incoming_rpcs -= 1;
  if (rs->completed) {
    // Here it might just be that we got a fragment back after we'd
    // already gotten enough to reconstruct the block.
    trace << myID << ": retrieve (" << rs->key
          << "): unexpected fragment from " << rs->succs[i] << ", discarding.\n";
    return;
  }

  if (!block) {
    trace << myID << ": retrieve (" << rs->key
	  << "): failed from successor " << rs->succs[i] << "\n";
    rs->errors++;
    fetch_frag (rs);
    return;
  }

  trace << myID << ": retrieve_verbose (" << rs->key << "): read from "
	<< rs->succs[i].x << "\n";
    
  
#ifdef VERBOSE_LOG  
  bigint h = compute_hash (block->data, block->len);
  trace << myID << ": retrieve (" << rs->key << ") got frag " << i
	<< " with hash " << h << " " << res->compl_res->res.size () << "\n";
#endif /* VERBOSE_LOG */
  
  str frag (block->data, block->len);
  for (size_t j = 0; j < rs->frags.size (); j++) {
    if (rs->frags[j] == frag) {
      warning << myID << ": retrieve (" << rs->key
	      << "): duplicate fragment retrieved from successor " << i+1
	      << "; same as fragment " << j << "\n";
      rs->errors++;
      fetch_frag (rs);
      return;
    }
  }
  rs->frags.push_back (frag);

  strbuf newblock;
  
  if (!Ida::reconstruct (rs->frags, newblock)) {
    if (rs->frags.size () >= dhash::num_dfrags ()) {
      warning << myID << ": retrieve (" << rs->key
	      << "): reconstruction failed.\n";
      rs->errors++;
      fetch_frag (rs);
    }
    return;
  }
  
  str tmp (newblock);
  if (!verify (rs->key.ID, rs->key.ctype, tmp.cstr (), tmp.len ())) {
    if (rs->frags.size () >= dhash::num_dfrags ()) {
      warning << myID << ": retrieve (" << rs->key
	      << "): verify failed.\n";
      rs->errors++;
      fetch_frag (rs);
    }
    return;
  }
  
  rs->timemark ();
  ptr<dhash_block> blk = 
    New refcounted<dhash_block> (tmp.cstr (), tmp.len (), rs->key.ctype);
  blk->ID = rs->key.ID;
  blk->hops = rs->r.size ();
  blk->errors = rs->errors;
  blk->retries = blk->errors;
  
  for (size_t i = 1; i < rs->times.size (); i++) {
    timespec diff = rs->times[i] - rs->times[i - 1];
    blk->times.push_back (diff.tv_sec * 1000 +
			  int (diff.tv_nsec/1000000));
  }
  
  rs->complete (DHASH_OK, blk);
  rs = NULL;
}


// Pull block fragments down from a successor list
// public interface, used by dhash_impl if completing a FETCHREC
void
dhashcli::assemble (blockID blockID, cb_ret cb, vec<chord_node> succs, route r)
{
  ptr<rcv_state> rs = New refcounted<rcv_state> (blockID, cb);
  rs->r = r;
  if (dhash_tcp_transfers)
    rs->succopt = true;
  doassemble (rs, succs);
}

void
dhashcli::doassemble (ptr<rcv_state> rs, vec<chord_node> succs)
{
  chordID myID = clntnode->my_ID ();

  //  while (succs.size () > dhash::num_efrags ()) {
  //  succs.pop_back ();
  // }

  if (succs.size () < dhash::num_dfrags ()) {
    warning << myID << ": retrieve (" << rs->key << "): "
	    << "insufficient number of successors returned!\n";
    rs->complete (DHASH_CHORDERR, NULL); // failure
    rs = NULL;
    return;
  }

  if (ordersucc_) {
    ptr<locationtable> lt = NULL;
    if (rs->succopt)
      lt = clntnode->locations;
    
    // Store list of successors ordered by expected distance.
    // fetch_frag will pull from this list in order.
#ifdef VERBOSE_LOG
    modlogger ("orderer", modlogger::TRACE) << "ordering for block "
					    << rs->key << "\n";
#endif /* VERBOSE_LOG */    
    order_succs (lt, clntnode->my_location ()->coords (),
		 succs, rs->succs, dhash::num_efrags ());
  } else {
    rs->succs = succs;
  }

  // Dispatch NUM_DFRAGS parallel requests, even though we don't know
  // how many fragments will truly be needed.
  u_int tofetch = dhash::num_dfrags () + 0;
  if (tofetch > rs->succs.size ()) tofetch = rs->succs.size ();
  for (u_int i = 0; i < tofetch; i++)
    fetch_frag (rs);
}

void
dhashcli::insert (ref<dhash_block> block, cbinsert_path_t cb, 
		  int options, ptr<chordID> guess)
{
  if (!guess) 
    lookup (block->ID, wrap (this, &dhashcli::insert_lookup_cb, block, cb, 
			     options));
  else { 
    ptr<location> l =  clntnode->locations->lookup (*guess);
    if (!l) {
      lookup (block->ID, 
	      wrap (this, &dhashcli::insert_lookup_cb, block, cb, options));
    } else
      clntnode->get_succlist (l, wrap (this, &dhashcli::insert_succlist_cb, 
				       block, cb, *guess, options));
  }
}

void
dhashcli::insert_succlist_cb (ref<dhash_block> block, cbinsert_path_t cb,
			      chordID guess, int options,
			      vec<chord_node> succs, chordstat status)
{
  if (status) {
    vec<chordID> rrr;
    cb (DHASH_CHORDERR, rrr);
    info << "insert_succlist_cb: failure (" << block->ID << "): "
	 << status << "\n";
    return;
  }

  route r;
  r.push_back (clntnode->locations->lookup (guess));
  insert_lookup_cb (block, cb, options, DHASH_OK, succs, r);
}

u_int64_t start_insert, end_insert, total_insert = 0;

void
dhashcli::insert_lookup_cb (ref<dhash_block> block, cbinsert_path_t cb, int options, 
			    dhash_stat status, vec<chord_node> succs, route r)
{
  vec<chordID> mt;
  if (status) {
    (*cb) (DHASH_CHORDERR, mt);
    return;
  }

  // benjie: if number of successors is smaller than num_efrags,
  // that's likely an indication that the ring is small.
  if (succs.size () < dhash::num_efrags ()) {
    chord_node s;
    clntnode->my_location ()->fill_node (s);

    bool in_succ_list = false;
    for (unsigned i=0; i<succs.size () && !in_succ_list; i++)
      if (succs [i].x == s.x)
	in_succ_list = true;

    if (in_succ_list) {
      // patch up the succ list so it includes all the node we know
      // of... mostly, this step will add the predecessor of the block
      vec<ptr<location> > sl = clntnode->succs ();
      merge_succ_list (succs, sl, dhash::num_efrags ());
    }
    else
      // clntnode is not in succ list, but there aren't enough
      // successors. this is probably an indication that the lookup is
      // done on clntnode itself.
      succs.push_back (s);
  }

  if (succs.size () < dhash::num_dfrags ()) {
    // this is a failure condition, since we can't hope to reconstruct
    // the block reliably.
    info << "Not enough successors for insert: |succs| " << succs.size ()
	 << ", DFRAGS " << dhash::num_dfrags () << "\n";
    (*cb) (DHASH_STOREERR, mt);
    return;
  }

  if (succs.size () < dhash::num_efrags ())
    // benjie: this is not a failure condition, since if we don't
    // receive num_efrags number of store replies in insert_store_cb,
    // we are still allowed to proceed.
    info << "Number of successors less than desired: |succs| " << succs.size ()
	 << ", EFRAGS " << dhash::num_efrags () << "\n";

  while (succs.size () > dhash::num_efrags ()) 
    succs.pop_back ();

  ref<sto_state> ss = New refcounted<sto_state> (block, cb);
  ss->succs = succs;
  
  timeval tp;
  gettimeofday (&tp, NULL);
  start_insert = tp.tv_sec * (u_int64_t) 1000000 + tp.tv_usec;

  if (block->ctype == DHASH_KEYHASH || block->ctype == DHASH_NOAUTH) {
    if (!DHC) {
      for (u_int i = 0; i < dhash::num_replica (); i++) {
	// Count up for each RPC that will be dispatched
	ss->out += 1;
	
	ptr<location> dest = clntnode->locations->lookup_or_create (succs[i]);
	dhash_store::execute (clntnode, dest, 
			      blockID(block->ID, block->ctype, DHASH_BLOCK),
			      block,
			      wrap (this, &dhashcli::insert_store_cb,  
				    ss, r, i, dhash::num_replica (), 
				    dhash::num_replica ()/2 + 1),
			      i == 0 ? DHASH_STORE : DHASH_REPLICA);
      }
    } else {
      ptr<location> dest = clntnode->locations->lookup_or_create (succs[0]);
      ptr<dhc_put_arg> arg = New refcounted<dhc_put_arg>;
      arg->bID = block->ID;
      arg->writer = clntnode->my_ID ();
      arg->value.set (block->data, block->len);
      arg->rmw = (options & DHASHCLIENT_RMW) ? 1 : 0;
      if (arg->rmw) { 
	bcopy (block->data, &arg->ctag.ver, sizeof (uint64));
	bzero (block->data + sizeof (uint64), sizeof (chordID));
	mpz_get_rawmag_be (block->data + sizeof (uint64), 
			   sizeof (chordID), &arg->ctag.writer);
	warn << "RMW: ver " << arg->ctag.ver << " writer " << arg->ctag.writer << "\n";
      }
      ptr<dhc_put_res> res = New refcounted<dhc_put_res>;

      if (options & DHASHCLIENT_NEWBLOCK)
	clntnode->doRPC (dest, dhc_program_1, DHCPROC_NEWBLOCK, arg, res,
			 wrap (this, &dhashcli::insert_dhc_cb, dest, r, ss->cb, res));
      else 
	clntnode->doRPC (dest, dhc_program_1, DHCPROC_PUT, arg, res,
			 wrap (this, &dhashcli::insert_dhc_cb, dest, r, ss->cb, res));	
    }
    return;
  }

  str blk (block->data, block->len);

  // Cap the maximum.
  u_long m = Ida::optimal_dfrag (block->len, dhash::dhash_mtu ());
  if (m > dhash::num_dfrags ())
    m = dhash::num_dfrags ();

  // trace << "Using m = " << m << " for block size " << block->len << "\n";

  for (u_int i = 0; i < succs.size (); i++) {
    str frag = Ida::gen_frag (m, blk);
    
    ref<dhash_block> blk = New refcounted<dhash_block> 
      ((char *)NULL, frag.len (), block->ctype);
    bcopy (frag.cstr (), blk->data, frag.len ());
    
    // Count up for each RPC that will be dispatched
    ss->out += 1;

    ptr<location> dest = clntnode->locations->lookup_or_create (succs[i]);

    dhash_store::execute (clntnode, dest,
			  blockID (block->ID, block->ctype, DHASH_FRAG),
			  blk,
			  wrap (this, &dhashcli::insert_store_cb,
				ss, r, i,
				dhash::num_efrags (), dhash::num_dfrags ()),
			  DHASH_FRAGMENT);
  }
}

void
dhashcli::insert_store_cb (ref<sto_state> ss, route r, u_int i,
			   u_int nstores, u_int min_needed,
			   dhash_stat err, chordID id, bool present)
{
  ss->out -= 1;
  if (err) {
    info << "fragment/block store failed: " << ss->block->ID
	 << " fragment " << i << "/" << nstores
	 << ": " << err << "\n";
  } else {
    info << "fragment/block store ok: " << ss->block->ID
	 << " fragment " << i << "/" << nstores
	 << " at " << id << "\n";
    ss->good += 1;
  }

  // Count down until all outstanding RPCs have returned
  vec<chordID> r_ret;

  if (ss->out == 0) {
    chordID myID = clntnode->my_ID ();
    if (ss->good < nstores) {
      warning << myID << ": store (" << ss->block->ID << "): only stored "
	      << ss->good << " of " << nstores << " encoded.\n";
      if (ss->good < min_needed) {
	warning << myID << ": store (" << ss->block->ID << "): failed;"
	  " insufficient frags/blocks stored.\n";
	
	r_ret.push_back (ss->succs[0].x);
	(*ss->cb) (DHASH_STOREERR, r_ret);
	// We should do something here to try and store this fragment
	// somewhere else.
	return;
      }
    }
    
    for (unsigned int i = 0; i < r.size (); i++)
      r_ret.push_back (r[i]->id ());

    (*ss->cb) (DHASH_OK, r_ret);
  }
}

void
dhashcli::insert_dhc_cb (ptr<location> dest, route r, cbinsert_path_t cb, 
			 ptr<dhc_put_res> res, clnt_stat cerr)
{
  vec<chordID> path;
  if (!cerr && res->status == DHC_OK) {
    for (uint i=0; i<r.size (); i++) 
      path.push_back (r[i]->id ());

    (*cb) (DHASH_OK, path);
  } else {
    warn << clntnode->my_ID () << "dhash err: " << cerr 
	 << " or dhc err: " << res->status << "\n";
    path.push_back (dest->id ());
    (*cb) (DHC_ERR, path);
  }
}

void
dhashcli::lookup (chordID blockID, dhashcli_lookupcb_t cb)
{
  clntnode->find_successor
    (blockID, wrap (this, &dhashcli::lookup_findsucc_cb, blockID, cb));
}

void
dhashcli::lookup_findsucc_cb (chordID blockID, dhashcli_lookupcb_t cb,
			      vec<chord_node> s, route path, chordstat err)
{
  if (err) 
    (*cb) (DHASH_CHORDERR, s, path);
  else
    (*cb) (DHASH_OK, s, path);
}


void
dhashcli::sendblock (ptr<location> dst, blockID bid, str data,
		     sendblockcb_t cb)
{
  ref<dhash_block> dhblk = New refcounted<dhash_block> 
    (data.cstr (), data.len (), bid.ctype);

  dhash_store::execute 
    (clntnode, dst, bid, dhblk,
     wrap (this, &dhashcli::sendblock_cb, cb),
     bid.dbtype == DHASH_BLOCK ? DHASH_REPLICA : DHASH_FRAGMENT);
}

void
dhashcli::sendblock (ptr<location> dst, blockID bid_to_send, ptr<dbfe> from_db,
		     sendblockcb_t cb)
{
  
  ptr<dbrec> blk = from_db->lookup (id2dbrec (bid_to_send.ID));
  if(!blk)
    cb (DHASH_NOTPRESENT, false);

  ref<dhash_block> dhblk = New refcounted<dhash_block> 
    (blk->value, blk->len, bid_to_send.ctype);

  dhash_store::execute 
    (clntnode, dst, bid_to_send, dhblk,
     wrap (this, &dhashcli::sendblock_cb, cb),
     bid_to_send.dbtype == DHASH_BLOCK ? DHASH_REPLICA : DHASH_FRAGMENT);
}

void
dhashcli::sendblock_cb (callback<void, dhash_stat, bool>::ref cb, 
			  dhash_stat err, chordID dest, bool present)
{
  (*cb) (err, present);
}
