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
#include <locationtable.h>

#include "dhash_common.h"
#include "dhash.h"
#include "dhashcli.h"
#include "verify.h"
#include "download.h"

#include <coord.h>
#include <misc_utils.h>
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

#define STORE_TIMEOUT 60

class dhash_store : public virtual refcount {
protected:
  uint npending;
  bool error;
  dhash_stat status;

  ptr<location> dest;
  chord_node pred_node;
  blockID blckID;
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
  timecb_t *dcb;
  bool returned;
  u_int64_t startt;

  void done ()
  {
    if (!returned && npending == 0) {
      (*cb) (status, dest->id ());
      returned = true;
    }
  }

  dhash_store (ptr<vnode> clntnode, ptr<location> dest, blockID blockID,
               dhash *dh, ptr<dhash_block> _block, store_status store_type, 
	       bool last, cbinsert_t cb)
    : dest (dest), blckID (blockID), dh (dh), block (_block), cb (cb),
      ctype (_block->ctype), store_type (store_type),
      clntnode (clntnode), num_retries (0), last (last)
  {
    startt = getusec ();
    returned = false;
    dcb = NULL;
    start ();
  }
  
  ~dhash_store ()
  {
    if (dcb)
      timecb_remove (dcb);
    dcb = NULL;
  }

  void timed_out ()
  {
    dcb = 0;
    error = true;
    status = DHASH_TIMEDOUT;
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
      store (dest, blckID, chunkdat, chunklen, chunkoff, 
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
      warn << "dhash_store failed: " << blckID << ": RPC error" << "\n";
    } 
    else if (res->status != DHASH_OK) {
      if (res->status == DHASH_RETRY) {
	pred_node = make_chord_node (res->pred->p);
      }
      else if (res->status != DHASH_WAIT)
        warn << "dhash_store failed: " << blckID << ": "
	     << dhasherr2str(res->status) << "\n";
      if (!error)
	status = res->status;
      error = true;
    }
    else { 
      //      warn << blckID << "(store timing) got store reply for " << num << " at " << (getusec () - startt) << "\n";
      if ((num > nextblock) && (numblocks - num > 1)) {
	warn << "(store) FAST retransmit: " << blckID << " got " 
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
	ptr<location> pn = clntnode->locations->lookup_or_create (pred_node);
	if (!pn && !returned) {
	  (*cb) (DHASH_CHORDERR, dest->id ());
	  returned = true;
	  return;
	}
	num_retries++;
	if (num_retries > 2) {
	  if (!returned) {
	    (*cb)(DHASH_RETRY, dest->id ());
	    returned = true;
	    return;
	  }
	} else {
	  warn << "retrying (" << num_retries << "): dest was " 
	       << dest->id () << " now is " << pred_node.x << "\n";
	  dest = pn;
	  start ();
	}
      }
      else
	done ();
    }
  }


  void store (ptr<location> dest, blockID blockID, char *data, size_t len,
	      size_t off, size_t totsz, int num, dhash_ctype ctype, 
	      store_status store_type)
  {
    ref<dhash_storeres> res = New refcounted<dhash_storeres> (DHASH_OK);
    ref<s_dhash_insertarg> arg = New refcounted<s_dhash_insertarg> ();
    arg->key     = blockID.ID;
    arg->ctype   = blockID.ctype;
    arg->dbtype  = blockID.dbtype;
    clntnode->my_location ()->fill_node (arg->from);
    arg->data.setsize (len);
    memcpy (arg->data.base (), data, len);
    arg->offset  = off;
    arg->type    = store_type;
    arg->nonce   = 0; // XXX remove!
    arg->attr.size     = totsz;
    arg->last    = last;
    
    //    warn << blockID << "(store timing) store RPC for " << num << " at " << (getusec () - startt) << "\n";
    long rexmitid = clntnode->doRPC
      (dest, dhash_program_1, DHASHPROC_STORE, arg, res,
       wrap (mkref(this), &dhash_store::finish, res, num));
    seqnos.push_back (rexmitid);
  }
  
public:
  
  static void execute (ptr<vnode> clntnode, ptr<location> dest, blockID blockID,
                       dhash *dh, ref<dhash_block> block, bool last,
		       cbinsert_t cb, store_status store_type = DHASH_STORE)
  {
    ptr<dhash_store> d = New refcounted<dhash_store> 
      (clntnode, dest, blockID, dh, block, store_type, last, cb);
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
dhashcli::retrieve (blockID blockID, cb_ret cb, int options, 
		    ptr<chordID> guess)
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

    route_iterator *ci = r_factory->produce_iterator_ptr (blockID.ID);

    if(blockID.ctype == DHASH_KEYHASH) {
      ci->first_hop (wrap (this, &dhashcli::retrieve_block_hop_cb, blockID, ci,
			   options, 5, guess),
		     guess);
    } else {
      ci->first_hop (wrap (this, &dhashcli::retrieve_frag_hop_cb, blockID, ci),
		     guess);
    }
  }
}

void
dhashcli::retrieve_block_hop_cb (blockID blockID, route_iterator *ci,
				 int options, int retries, ptr<chordID> guess,
				 bool done)
{
  if (!done) {
    ci->next_hop ();
    return;
  }

  chordID myID = clntnode->my_ID ();
  rcv_state *rs = rcvs[blockID];
  if (!rs) {
    trace << myID << ": retrieve (" << blockID << "): not in table?\n";
    assert (rs);
  }
  rs->timemark ();
  rs->r = ci->path ();
  rs->succs = ci->successors ();
  dhash_stat status = ci->status () ? DHASH_CHORDERR : DHASH_OK;
  delete ci;
  
  if (status != DHASH_OK) {
    trace << myID << ": retrieve (" << blockID << "): lookup failure: " << status << "\n";
    rcvs.remove (rs);
    rs->complete (status, NULL); // failure
    rs = NULL;    
    return;
  }

  chord_node s = rs->succs.pop_front ();
  dhash_download::execute (clntnode, s, blockID, NULL, 0, 0, 0,
			   wrap (this, &dhashcli::retrieve_dl_or_walk_cb,
				 blockID, status, options, retries, guess));
}

void
dhashcli::retrieve_dl_or_walk_cb (blockID blockID, dhash_stat status,
				  int options, int retries, ptr<chordID> guess,
				  ptr<dhash_block> blk)
{
  chordID myID = clntnode->my_ID ();
  rcv_state *rs = rcvs[blockID];
  if (!rs) {
    trace << myID << ": retrieve (" << blockID << "): not in table?\n";
    assert (rs);
  }

  if(!blk) {
    if (options & DHASHCLIENT_NO_RETRY_ON_LOOKUP) {
      rcvs.remove (rs);
      rs->complete (DHASH_NOENT, NULL);
      rs = NULL;
    } else if (rs->succs.size() == 0) {
      warn << "walk: No luck walking successors, retrying..\n";
      route_iterator *ci = r_factory->produce_iterator_ptr (blockID.ID);
      delaycb (5, wrap (ci, &route_iterator::first_hop, 
			wrap (this, &dhashcli::retrieve_block_hop_cb,
			      blockID, ci, options, retries - 1, guess),
			guess));
    } else {
      chord_node s = rs->succs.pop_front ();
      dhash_download::execute (clntnode, s, blockID, NULL, 0, 0, 0,
			       wrap (this, &dhashcli::retrieve_dl_or_walk_cb,
				     blockID, status, options, retries,
				     guess));
    }
  } else {
    rs->timemark ();

    blk->ID = rs->key.ID;
    blk->hops = rs->r.size ();
    blk->errors = rs->nextsucc - dhash::NUM_DFRAGS;
    blk->retries = blk->errors;

    rcvs.remove (rs);
    rs->complete (DHASH_OK, blk);
    rs = NULL;
  }
}

void
dhashcli::retrieve_frag_hop_cb (blockID blockID, route_iterator *ci, bool done)
{
  vec<chord_node> cs = ci->successors ();
  if (done) {
    route r = ci->path ();
    dhash_stat stat = ci->status () ? DHASH_CHORDERR : DHASH_OK;
    delete ci;
    retrieve_lookup_cb (blockID, stat, cs, r);
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
      if (betweenrightincl (cs[i-1].x, cs[i].x, blockID.ID)) {
	cs.popn_front (i);
	route r = ci->path ();
	delete ci;
	
	chordID myID = clntnode->my_ID ();
	trace << myID << ": retrieve (" << blockID << "): skipping " << i
	      << " nodes.\n";
	retrieve_lookup_cb (blockID, DHASH_OK, cs, r);
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
  
  rs->incoming_rpcs += 1;
  dhash_download::execute (clntnode, rs->succs[i], 
			   blockID(rs->key.ID, rs->key.ctype, DHASH_FRAG),
			   (char *)NULL, 0, 0, 0, 
			   wrap (this, &dhashcli::retrieve_fetch_cb, rs->key, i));
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
dhashcli::retrieve_lookup_cb (blockID blockID,
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
    order_succs (clntnode->my_location ()->coords (),
		 succs, rs->succs);
  } else {
    rs->succs = succs;
  }

  for (u_int i = 0; i < dhash::NUM_DFRAGS; i++)
    fetch_frag (rs);
}

void
dhashcli::retrieve_fetch_cb (blockID blockID, u_int i,
			     ptr<dhash_block> block)
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

  if (!block) {
    trace << myID << ": retrieve (" << blockID
	  << "): failed from successor " << i+1 << "\n";
    fetch_frag (rs);
    return;
  }
  
  bigint h = compute_hash (block->data, block->len);
#ifdef VERBOSE_LOG  
  trace << myID << ": retrieve (" << blockID << ") got frag " << i
	<< " with hash " << h << " " << res->compl_res->res.size () << "\n";
#endif /* VERBOSE_LOG */
  
  str frag (block->data, block->len);
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
      New refcounted<dhash_block> (tmp.cstr (), tmp.len (), rs->key.ctype);
    blk->ID = rs->key.ID;
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
dhashcli::insert (ref<dhash_block> block, cbinsert_path_t cb, 
		  int options, ptr<chordID> guess)
{
  if (!guess)
    lookup (block->ID, 
	    wrap (this, &dhashcli::insert_lookup_cb, block, cb));
  else { 
    ptr<location> l =  clntnode->locations->lookup (*guess);
    if (!l) {
      lookup (block->ID, 
	      wrap (this, &dhashcli::insert_lookup_cb, block, cb));
    } else
      clntnode->get_succlist (l, wrap (this, &dhashcli::insert_succlist_cb, 
				       block, cb, *guess));
  }
}

void
dhashcli::insert_succlist_cb (ref<dhash_block> block, cbinsert_path_t cb,
			      chordID guess,
			      vec<chord_node> succs, chordstat status)
{
  if (status) {
    vec<chordID> rrr;
    cb (DHASH_CHORDERR, rrr);
    warn << "succlist: failure\n";
    return;
  }

  route r;
  r.push_back (clntnode->locations->lookup (guess));
  insert_lookup_cb (block, cb, DHASH_OK, succs, r);
}

void
dhashcli::insert_lookup_cb (ref<dhash_block> block, cbinsert_path_t cb, 
			    dhash_stat status, vec<chord_node> succs, route r)
{
  vec<chordID> mt;
  if (status) {
    (*cb) (DHASH_CHORDERR, mt);
    return;
  }

  ref<sto_state> ss = New refcounted<sto_state> (block, cb);
  ss->succs = succs;
  
  if (block->ctype == DHASH_KEYHASH) {
    for (u_int i = 0; i < succs.size (); i++) {
      // Count up for each RPC that will be dispatched
      ss->out += 1;

      ptr<location> dest = clntnode->locations->lookup_or_create (succs[i]);
      dhash_store::execute (clntnode, dest, 
			    blockID(block->ID, block->ctype, DHASH_BLOCK),
			    dh, block, false,  
			    wrap (this, &dhashcli::insert_store_cb,  
				  ss, r, i,
				  ss->succs.size (), ss->succs.size () / 2),
			    DHASH_CACHE);  // XXX really CACHE??
    }
    return;
  }

  if (dhash::NUM_EFRAGS > succs.size ()) {
    warn << "Not enough successors: |succs| " << succs.size ()
	 << ", EFRAGS " << dhash::NUM_EFRAGS << "\n";
    (*cb) (DHASH_STOREERR, mt); // XXX Not the right error code...
    return;
  }
  
  str blk (block->data, block->len);

  for (u_int i = 0; i < dhash::NUM_EFRAGS; i++) {
    assert (i < succs.size ());
    str frag = Ida::gen_frag (dhash::NUM_DFRAGS, blk);
    
    ref<dhash_block> blk = New refcounted<dhash_block> ((char *)NULL, frag.len (), DHASH_CONTENTHASH);
    bcopy (frag.cstr (), blk->data, frag.len ());
    
    bigint h = compute_hash (blk->data, blk->len);
    warnx << "Put frag: " << i << " " << h << " " << blk->len << " to " << succs[i].x << "\n";
    
    // Count up for each RPC that will be dispatched
    ss->out += 1;

    ptr<location> dest = clntnode->locations->lookup_or_create (succs[i]);
    dhash_store::execute (clntnode, dest,
			  blockID(block->ID, block->ctype, DHASH_FRAG),
			  dh, blk, false,
			  wrap (this, &dhashcli::insert_store_cb,
				ss, r, i,
				dhash::NUM_EFRAGS, dhash::NUM_DFRAGS),
			  DHASH_CACHE);
  }
}

void
dhashcli::insert_store_cb (ref<sto_state> ss, route r, u_int i,
			   u_int nstores, u_int min_needed,
			   dhash_stat err, chordID id)
{
  ss->out -= 1;
  if (err) {
    warnx << "fragment/block store failed: " << ss->block->ID
	  << " fragment " << i << "/" << nstores
	  << ": " << err << "\n";
  } else {
    ss->good += 1;
  }

  // Count down until all outstanding RPCs have returned
  vec<chordID> r_ret;

  if (ss->out == 0) {
    chordID myID = clntnode->my_ID ();
    if (ss->good < nstores) {
      trace << myID << ": store (" << ss->block->ID << "): only stored "
	    << ss->good << " of " << nstores << " encoded.\n";
      if (ss->good < min_needed) {
	trace << myID << ": store (" << ss->block->ID << "): failed;"
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


//like insert, but doesn't do lookup. used by transfer_key
void
dhashcli::storeblock (ptr<location> dest, chordID ID, ref<dhash_block> block, 
		      bool last, cbinsert_t cb, store_status stat)
{
  dhash_store::execute (clntnode, dest, blockID(ID, block->ctype, DHASH_BLOCK),
			dh, block, last, cb, stat);
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


