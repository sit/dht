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
#include <location.h>

#include "dhash_common.h"
#include "dhash.h"
#include "dhashcli.h"
#include "verify.h"
#include "route_dhash.h"

#include <coord.h>
#include <modlogger.h>
#define trace modlogger ("dhashcli")

#ifdef DMALLOC
#include <dmalloc.h>
#endif
#include <ida.h>

// ---------------------------------------------------------------------------
// DHASH_STORE
//     - store a give block of data on a give nodeID.
//     - the address of the nodeID must already by in the location cache.
//     - XXX don't really handle RACE conditions..

static uint32 store_nonce = 0;
#define STORE_TIMEOUT 60

class dhash_store : public virtual refcount {
protected:
  uint npending;
  bool error;
  dhash_stat status;

  chordID destID;
  chord_node pred_node;
  uint32 nonce;
  chordID blockID;
  dhash *dh;
  ptr<dhash_block> block;
  cbinsert_t cb;
  dhash_ctype ctype;
  store_status store_type;
  ptr<vnode> clntnode;
  int num_retries;
  bool last;
  int nextblock;
  int numblocks;
  vec<long> seqnos;
  bool got_storecb;
  timecb_t *dcb;
  bool returned;
  u_int64_t startt;

  void done ()
  {
    if (!returned && got_storecb && npending == 0) {
      (*cb) (status, destID);
      returned = true;
    }
  }

  void storecb_cb (s_dhash_storecb_arg *arg)
  {
    got_storecb = true;

    //    warn << blockID << "(store timing) got store callback for at " << (getusec () - startt) << "\n";
    if (arg->status) {
      if (!error)
	status = arg->status;
      error = true;
    }

    done ();
  }

  dhash_store (ptr<vnode> clntnode, chordID destID, chordID blockID,
               dhash *dh, ptr<dhash_block> _block, store_status store_type, 
	       bool last, cbinsert_t cb)
    : destID (destID), blockID (blockID), dh (dh), block (_block), cb (cb),
      ctype (block_type(_block)), store_type (store_type),
      clntnode (clntnode), num_retries (0), last (last)
  {
    startt = getusec ();
    returned = false;
    dcb = NULL;
    if (store_type == DHASH_STORE) {
      nonce = store_nonce++;
      dh->register_storecb_cb
	(nonce, wrap (mkref(this), &dhash_store::storecb_cb));
    }
    start ();
  }
  
  ~dhash_store ()
  {
    if (dcb)
      timecb_remove (dcb);
    dcb = NULL;
    if (store_type == DHASH_STORE)
      dh->unregister_storecb_cb (nonce);
  }

  void timed_out ()
  {
    dcb = 0;
    error = true;
    status = DHASH_TIMEDOUT;
    got_storecb = true;
    npending = 0;
    done ();
  }
    
  void start ()
  {
    error = false;
    status = DHASH_OK;
    npending = 0;
    nextblock = 0;
    numblocks = 0;
    int blockno = 0;
    got_storecb = true;
    if (store_type == DHASH_STORE)
      got_storecb = false;
  
    if (dcb)
      timecb_remove (dcb);

    dcb = delaycb
      (STORE_TIMEOUT, wrap (mkref(this), &dhash_store::timed_out));

    size_t nstored = 0;
    while (nstored < block->len) {
      size_t chunklen = MIN (MTU, block->len - nstored);
      char  *chunkdat = &block->data[nstored];
      size_t chunkoff = nstored;
      npending++;
      store (destID, blockID, chunkdat, chunklen, chunkoff, 
	     block->len, blockno, ctype, store_type);
      nstored += chunklen;
      blockno++;
    }
    numblocks = blockno;
  }

  void finish (ptr<dhash_storeres> res, int num, clnt_stat err)
  {
    npending--;

    if (err) {
      error = true;
      got_storecb = true;
      warn << "dhash_store failed: " << blockID << ": RPC error" << "\n";
    } 
    else if (res->status != DHASH_OK) {
      if (res->status == DHASH_RETRY) {
	pred_node =res->pred->p;
      }
      else if (res->status != DHASH_WAIT)
        warn << "dhash_store failed: " << blockID << ": "
	     << dhasherr2str(res->status) << "\n";
      if (!error)
	status = res->status;
      error = true;
      got_storecb = true;
    }
    else { 
      //      warn << blockID << "(store timing) got store reply for " << num << " at " << (getusec () - startt) << "\n";
      if ((num > nextblock) && (numblocks - num > 1)) {
	warn << "(store) FAST retransmit: " << blockID << " got " 
	     << num << " chunk " << nextblock << " of " << numblocks 
	     << " being retransmitted\n";
	clntnode->resendRPC(seqnos[nextblock]);
	//only one per fetch; finding more is too much bookkeeping
	numblocks = -1;
      }
      nextblock++;
    }

    if (npending == 0) {
      if (status == DHASH_RETRY) {
	bool ok = clntnode->locations->insert (pred_node);
	if (!ok && !returned) {
	  (*cb) (DHASH_CHORDERR, destID);
	  returned = true;
	  return;
	}
	num_retries++;
	if (num_retries > 2) {
	  if (!returned) {
	    (*cb)(DHASH_RETRY, destID);
	    returned = true;
	    return;
	  }
	} else {
	  warn << "retrying (" << num_retries << "): dest was " 
	       << destID << " now is " << pred_node.x << "\n";
	  destID = pred_node.x;
	  start ();
	}
      }
      else
	done ();
    }
  }


  void store (chordID destID, chordID blockID, char *data, size_t len,
	      size_t off, size_t totsz, int num, dhash_ctype ctype, 
	      store_status store_type)
  {
    ref<dhash_storeres> res = New refcounted<dhash_storeres> (DHASH_OK);
    ref<s_dhash_insertarg> arg = New refcounted<s_dhash_insertarg> ();
    arg->key     = blockID;
    arg->srcID   = clntnode->my_ID ();
    clntnode->locations->get_node (arg->srcID, &arg->from);
    arg->data.setsize (len);
    memcpy (arg->data.base (), data, len);
    arg->offset  = off;
    arg->type    = store_type;
    arg->nonce   = nonce;
    arg->attr.size     = totsz;
    arg->last    = last;
    
    //    warn << blockID << "(store timing) store RPC for " << num << " at " << (getusec () - startt) << "\n";
    long rexmitid = clntnode->doRPC
      (destID, dhash_program_1, DHASHPROC_STORE, arg, res,
       wrap (mkref(this), &dhash_store::finish, res, num));
    seqnos.push_back (rexmitid);
  }
  
public:
  
  static void execute (ptr<vnode> clntnode, chordID destID, chordID blockID,
                       dhash *dh, ref<dhash_block> block, bool last,
		       cbinsert_t cb, store_status store_type = DHASH_STORE)
  {
    ptr<dhash_store> d = New refcounted<dhash_store> 
      (clntnode, destID, blockID, dh, block, store_type, last, cb);
  }
};




// ---------------------------------------------------------------------------
// DHASHCLI

 
dhashcli::dhashcli (ptr<vnode> node, dhash *dh, ptr<route_factory> r_factory,
                    bool do_cache, int ss_mode)
  : clntnode (node), do_cache (do_cache),
    server_selection_mode (ss_mode), dh (dh), r_factory (r_factory)
{
}

void
dhashcli::retrieve2 (chordID blockID, int options, cb_ret cb)
{
  // Only one lookup for a block should be in progress from a node
  // at any given time.
  chordID myID = clntnode->my_ID ();
  rcv_state *rs = rcvs[blockID];
  if (rs) {
#ifdef VERBOSE_LOG    
    trace << myID << ": retrieve (" << blockID << "): simultaneous retrieve!\n";
#endif /* VERBOSE_LOG */    
    rs->callbacks.push_back (cb);
  } else {
#ifdef VERBOSE_LOG    
    trace << myID << ": retrieve (" << blockID << "): new retrieve.\n";
#endif /* VERBOSE_LOG */    
    rs = New rcv_state (blockID);
    rs->callbacks.push_back (cb);
    rcvs.insert (rs);

    route_iterator *ci = r_factory->produce_iterator_ptr (blockID);
    ci->first_hop (wrap (this, &dhashcli::retrieve2_hop_cb, blockID, ci));
  }
}

void
dhashcli::retrieve2_hop_cb (chordID blockID, route_iterator *ci, bool done)
{
  vec<chord_node> cs = ci->successors ();
  if (done) {
    route r = ci->path ();
    dhash_stat stat = ci->status () ? DHASH_CHORDERR : DHASH_OK;
    delete ci;
    retrieve2_lookup_cb (blockID, stat, cs, r);
    return;
  }
  // Check to see if we already have enough in our successors.
  if (server_selection_mode & 4) {
    size_t left = 0;
    // XXX + 2? geez.
    if (cs.size () < dhash::NUM_DFRAGS + 2)
      left = cs.size ();
    else
      left = cs.size () - (dhash::NUM_DFRAGS + 2);
    for (size_t i = 1; i < left; i++) {
      if (betweenleftincl (cs[i-1].x, cs[i].x, blockID)) {
	cs.popn_front (i);
	route r = ci->path ();
	delete ci;
	
	chordID myID = clntnode->my_ID ();
	trace << myID << ": retrieve (" << blockID << "): skipping " << i
	      << " nodes.\n";
	retrieve2_lookup_cb (blockID, DHASH_OK, cs, r);
	return;
      }
    }
  }
  
  ci->next_hop ();
}

void
dhashcli::fetch_frag (rcv_state *rs)
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
    trace << myID << ": retrieve (" << rs->key << "): out of successors; failing.\n";
    rcvs.remove (rs);
    rs->complete (DHASH_NOENT, NULL);
    rs = NULL;
    return;
  }
  
  ref<dhash_fetchiter_res> res = New refcounted<dhash_fetchiter_res> (DHASH_OK);
  ref<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg> ();
  
  clntnode->locations->get_node (clntnode->my_ID (), &arg->from);
  arg->key    = rs->key;
  arg->start  = 0;
  arg->len    = 65536; // 64K is about as big as an IP packet will go...
  arg->cookie = 0;
  arg->nonce  = 0;
  
  rs->incoming_rpcs += 1;
  clntnode->doRPC (rs->succs[i], dhash_program_1, DHASHPROC_FETCHITER,
		   arg, res,
		   wrap (this, &dhashcli::retrieve2_fetch_cb, rs->key, i, res));  
  rs->nextsucc += 1;
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
order_succs (const vec<float> &me, const vec<chord_node> &succs,
	     vec<chord_node> &out)
{
  orderer *d2me = New orderer[succs.size()];
  vec<float> cursucc;
  for (size_t i = 0; i < succs.size (); i++) {
    cursucc.setsize (succs[i].coords.size ());
    for (size_t j = 0; j < succs[i].coords.size (); j++) 
      cursucc[j] = (float) succs[i].coords[j] / 1000.0;
    d2me[i].d_ = Coord::distance_f (me, cursucc);
    d2me[i].i_ = i;
  }
  qsort (d2me, succs.size (), sizeof (*d2me), &orderer::cmp);
  out.clear ();
  for (size_t i = 0; i < succs.size (); i++) {
#ifdef VERBOSE_LOG
    char buf[10]; // argh. please shoot me.
    sprintf (buf, "%5.2f", d2me[i].d_);
    modlogger ("orderer") << d2me[i].i_ << " "
			  << succs[d2me[i].i_] << " "
			  << buf << "\n";
#endif /* VERBOSE_LOG */    
    out.push_back (succs[d2me[i].i_]);
  }
}



void
dhashcli::retrieve2_lookup_cb (chordID blockID,
			       dhash_stat status,
			       vec<chord_node> succs,
			       route r)
{
  chordID myID = clntnode->my_ID ();
  rcv_state *rs = rcvs[blockID];
  if (!rs) {
    trace << myID << ": retrieve (" << blockID << "): not in table?\n";
    assert (rs);
  }
  rs->timemark ();
  rs->r = r;
  
  if (status != DHASH_OK) {
    trace << myID << ": retrieve (" << blockID << "): lookup failure: " << status << "\n";
    rcvs.remove (rs);
    rs->complete (status, NULL); // failure
    rs = NULL;    
    return;
  }
  
  while (succs.size () > dhash::NUM_EFRAGS)
    succs.pop_back ();

  if (succs.size () < dhash::NUM_DFRAGS) {
    trace << myID << ": retrieve (" << blockID << "): "
	  << "insufficient number of successors returned!\n";
    rcvs.remove (rs);
    rs->complete (DHASH_CHORDERR, NULL); // failure
    rs = NULL;
    return;
  }

  if (server_selection_mode & 1) {
    // Store list of successors ordered by expected distance.
    // fetch_frag will pull from this list in order.
#ifdef VERBOSE_LOG    
    modlogger ("orderer") << "ordering for block " << blockID << "\n";
#endif /* VERBOSE_LOG */    
    order_succs (clntnode->locations->get_coords (clntnode->my_ID ()),
		 succs, rs->succs);
  } else {
    rs->succs = succs;
  }

  for (u_int i = 0; i < dhash::NUM_DFRAGS; i++)
    fetch_frag (rs);
}

void
dhashcli::retrieve2_fetch_cb (chordID blockID, u_int i,
			      ref<dhash_fetchiter_res> res,
			      clnt_stat err)
{
  chordID myID = clntnode->my_ID ();
  // XXX collect fragments and decode block
  rcv_state *rs = rcvs[blockID];
  if (!rs) {
    // Here it might just be that we got a fragment back after we'd
    // already gotten enough to reconstruct the block.
    trace << myID << ": retrieve (" << blockID << "): unexpected fragment from "
	  << i + 1 << ", discarding.\n";
    return;
  }

  rs->incoming_rpcs -= 1;

  if (err) {
    trace << myID << ": retrieve (" << blockID
	  << "): failed from successor " << i+1  << ": " << err << "\n";
    fetch_frag (rs);
    return;
  } else if (res->status != DHASH_COMPLETE) {
    trace << myID << ": retrieve (" << blockID
	  << "): failed from successor " << i+1 << ": "
	  << dhasherr2str (res->status) << "\n";
    fetch_frag (rs);
    return;
  } else if (res->compl_res->attr.size > res->compl_res->res.size()) {
    trace << "we requested too short of a block!\n";
    assert (0);
  }
  
  bigint h = compute_hash (res->compl_res->res.base (), res->compl_res->res.size ());
#ifdef VERBOSE_LOG  
  trace << myID << ": retrieve (" << blockID << ") got frag " << i
	<< " with hash " << h << " " << res->compl_res->res.size () << "\n";
#endif /* VERBOSE_LOG */
  
  // strip off the 4 bytes header to get the fragment
  assert (res->compl_res->res.size () >= 4);
  str frag (res->compl_res->res.base () + 4, res->compl_res->res.size () - 4);
  rs->frags.push_back (frag);

  if (rs->frags.size () >= dhash::NUM_DFRAGS) {
    strbuf block;
    if (!Ida::reconstruct (rs->frags, block)) {
      trace << myID << ": retrieve (" << blockID << "): reconstruction failed.\n";
      fetch_frag (rs);
      return;
    }
    
    rs->timemark ();

    str tmp (block);
    ptr<dhash_block> blk = 
      New refcounted<dhash_block> (tmp.cstr (), tmp.len ());
    blk->ID = rs->key;
    blk->hops = rs->r.size ();
    blk->errors = rs->nextsucc - dhash::NUM_DFRAGS;
    blk->retries = blk->errors;

    for (size_t i = 1; i < rs->times.size (); i++) {
      timespec diff = rs->times[i] - rs->times[i - 1];
      blk->times.push_back (diff.tv_sec * 1000 +
			    int (diff.tv_nsec/1000000));
    }
    
    rcvs.remove (rs);
    rs->complete (DHASH_OK, blk);
    rs = NULL;
  }
}


void
dhashcli::retrieve (chordID blockID, int options, cb_ret cb)
{
  ///warn << "dhashcli::retrieve\n";
  ref<route_dhash> iterator =
    New refcounted<route_dhash> (r_factory, blockID, dh, options);
  iterator->execute (wrap (this, &dhashcli::retrieve_hop_cb, cb, blockID));
}

void
dhashcli::retrieve_hop_cb (cb_ret cb, chordID key,
			   dhash_stat status, 
			   ptr<dhash_block> blk, 
			   route path) 
{
  if (status) {
    warn << "iterator exiting w/ status\n";
    (*cb) (status, NULL, path);
  } else {
    cb (status, blk, path); 
    cache_block (blk, path, key);
  }
}

void
dhashcli::cache_block (ptr<dhash_block> block, route search_path, chordID key)
{

  if (block && do_cache && 
      (block_type (block) == DHASH_CONTENTHASH)) {
    unsigned int path_size = search_path.size ();
    if (path_size > 1) {
      chordID cache_dest = search_path[path_size - 2];
      dhash_store::execute (clntnode, cache_dest, key, dh, block, false,
			    wrap (this, &dhashcli::finish_cache),
			    DHASH_CACHE);
    }
    
  }
}

void
dhashcli::finish_cache (dhash_stat status, chordID dest)
{
  if (status != DHASH_OK)
    warn << "error caching block\n";
}

//use this version if you already know where the block is (and guessing
// that you have the successor cached won't work)
void
dhashcli::retrieve (chordID source, chordID blockID, cb_ret cb)
{
  ref<route_dhash> iterator = New refcounted<route_dhash>(r_factory, 
							 blockID, dh);
  iterator->execute (wrap (this, &dhashcli::retrieve_with_source_cb, cb), 
		     source);  
}

void
dhashcli::retrieve_with_source_cb (cb_ret cb, dhash_stat status, 
				   ptr<dhash_block> block, route path)
{
  if (status) 
    (*cb) (status, NULL, path);
  else 
    cb (status, block, path); 
}


/* Insert using coding */ 
void
dhashcli::insert2 (ref<dhash_block> block, int options, cbinsert_t cb)
{
  lookup (block->ID, options,
          wrap (this, &dhashcli::insert2_lookup_cb, block, cb));
}

void
dhashcli::insert2_lookup_cb (ref<dhash_block> block, cbinsert_t cb, 
			     dhash_stat status, vec<chord_node> succs, route r)
{
  if (status) {
    (*cb) (DHASH_CHORDERR, 0);
    return;
  }

  ref<sto_state> ss = New refcounted<sto_state> (block, cb);
  ss->succs = succs;
  
  if (dhash::NUM_EFRAGS > succs.size ()) {
    warn << "Not enough successors: |succs| " << succs.size ()
	 << ", EFRAGS " << dhash::NUM_EFRAGS << "\n";
    (*cb) (DHASH_STOREERR, 0); // XXX Not the right error code...
    return;
  }
  
  str blk (block->data, block->len);
  
  for (u_int i = 0; i < dhash::NUM_EFRAGS; i++) {
    assert (i < succs.size ());
    str frag = Ida::gen_frag (dhash::NUM_DFRAGS, blk);
    
    ref<s_dhash_insertarg> arg = New refcounted<s_dhash_insertarg> ();
    
    arg->key = block->ID;  // frag stored under hash(block)
    arg->srcID = clntnode->my_ID ();
    clntnode->locations->get_node (arg->srcID, &arg->from);
    
    // prepend type of block onto fragment and copy into arg.
    int realfraglen = frag.len () + 4;
    arg->data.setsize (realfraglen);
    bcopy (block->data, arg->data.base (), 4);
    bcopy (frag.cstr (), arg->data.base () + 4, frag.len());
    
    arg->offset  = 0;
    arg->type    = DHASH_CACHE; // XXX bit of a hack..see server.C::dispatch()
    arg->nonce   = 0;
    arg->attr.size  = realfraglen;
    arg->last    = false;
    
    warn << "Frag " << i << " to " << succs[i].x << "\n";
    bigint h = compute_hash (arg->data.base (), arg->data.size ());
    warn << "Put frag: " << i << " " << h << " " << arg->data.size () << "\n";
      
    
    ref<dhash_storeres> res = New refcounted<dhash_storeres> (DHASH_OK);
    // Count up for each RPC that will be dispatched
    ss->out += 1;
    clntnode->doRPC (succs[i], dhash_program_1, DHASHPROC_STORE, 
		     arg, res,
		     wrap (this, &dhashcli::insert2_store_cb, ss, i, res));
  }
}

void
dhashcli::insert2_store_cb (ref<sto_state> ss, u_int i, ref<dhash_storeres> res,
			    clnt_stat err)
{
  ss->out -= 1;
  if (err) {
    warnx << "fragment store failed: " << ss->block->ID
	  << " fragment " << i << "/" << dhash::NUM_EFRAGS
	  << ": " << err << "\n";
  } else {
    ss->good += 1;
  }

  // Count down until all outstanding RPCs have returned
  if (ss->out == 0) {
    chordID myID = clntnode->my_ID ();
    if (ss->good < dhash::NUM_EFRAGS) {
      trace << myID << ": store (" << ss->block->ID << "): only stored " << ss->good
	    << " of " << dhash::NUM_EFRAGS << " encoded.\n";
      if (ss->good < dhash::NUM_DFRAGS) {
	trace << myID << ": store (" << ss->block->ID << "): failed;"
	  " insufficient frags stored.\n";
	(*ss->cb) (DHASH_STOREERR, ss->succs[0].x);
	// We should do something here to try and store this fragment
	// somewhere else.
	return;
      }
    }
    (*ss->cb) (DHASH_OK, ss->succs[0].x);
  }
}

void
dhashcli::insert (chordID blockID, ref<dhash_block> block, 
                  int options, cbinsert_t cb)
{
  lookup (blockID, options,
          wrap (this, &dhashcli::insert_lookup_cb, blockID, block, cb, 0));
}

void
dhashcli::insert_lookup_cb (chordID blockID, ref<dhash_block> block,
			    cbinsert_t cb, int trial,
			    dhash_stat status, vec<chord_node> succs, route r)
{
  chordID destID = succs[0].x;
  if (status != DHASH_OK) {
    warn << "insert_lookup_cb: failure\n";
    // XXX call dhashcli::insert_stored_cb() to retry
    (*cb) (status, bigint(0)); // failure
  } else 
    dhash_store::execute (clntnode, destID, blockID, dh, block, false, 
			  wrap (this, &dhashcli::insert_stored_cb, 
				blockID, block, cb, trial));
}

void
dhashcli::insert_stored_cb (chordID blockID, ref<dhash_block> block,
			    cbinsert_t cb, int trial,
			    dhash_stat stat, chordID retID)
{
  if (stat && stat != DHASH_WAIT && (trial <= 2)) {
    //try the lookup again if we got a RETRY
    warn << "got a RETRY failure (" << trial << "). Trying the lookup again\n";
    lookup (blockID, false, 
	    wrap (this, &dhashcli::insert_lookup_cb,
		  blockID, block, cb, trial + 1));
  } else {
    cb (stat, retID);
  }
}
//like insert, but doesn't do lookup. used by transfer_key
void
dhashcli::storeblock (chordID dest, chordID ID, ref<dhash_block> block, 
		      bool last, cbinsert_t cb, store_status stat)
{
  dhash_store::execute (clntnode, dest, ID, dh, block, last, cb, stat);
}


void
dhashcli::lookup (chordID blockID, int options, dhashcli_lookupcb_t cb)
{
  if (options & DHASHCLIENT_USE_CACHED_SUCCESSOR) {
    chordID x = clntnode->lookup_closestsucc (blockID);
    vec<chord_node, 1> y;
    clntnode->locations->get_node (x, &y[0]);
    (*cb) (DHASH_OK, y, route ());
  }
  else
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


