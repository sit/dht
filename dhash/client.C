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

#include <chord.h>
#include <dhash.h>
#include <chord_prot.h>
#include "sfsmisc.h"
#include "arpc.h"
#include "crypt.h"
#include <sys/time.h>
#include "chord_util.h"
#ifdef DMALLOC
#include "dmalloc.h"
#endif


// ---------------------------------------------------------------------------
// DHASH_STORE
//     - store a give block of data on a give nodeID.
//     - the address of the nodeID must already by in the location cache.
//     - XXX don't really handle RACE conditions..

static uint32 store_nonce = 0;

class dhash_store {
protected:
  uint npending;
  bool error;
  dhash_stat status;

  chordID destID;
  chordID predID;
  uint32 nonce;
  net_address pred_addr;
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

  void storecb_cb (s_dhash_storecb_arg *arg)
  {
    // warn << "STORECB_CB " << nonce << "\n";
    if (arg->status)
      (*cb) (arg->status, destID);
    else
      (*cb) (DHASH_OK, destID);
    delete this;
  }

  dhash_store (ptr<vnode> clntnode, chordID destID, chordID blockID,
               dhash *dh, ptr<dhash_block> _block, store_status store_type, 
	       bool last, cbinsert_t cb)
    : destID (destID), blockID (blockID), dh (dh), block (_block), cb (cb),
      ctype (block_type(_block)), store_type (store_type),
      clntnode (clntnode), num_retries (0), last (last)
  {
    if (store_type == DHASH_STORE) {
      nonce = store_nonce++;
      dh->register_storecb_cb (nonce, wrap (this, &dhash_store::storecb_cb));
    }
    start ();
  }
  
  ~dhash_store ()
  {
    if (store_type == DHASH_STORE)
      dh->unregister_storecb_cb (nonce);
  }
    
  void start ()
  {
    error = false;
    status = DHASH_OK;
    npending = 0;
    nextblock = 0;
    numblocks = 0;
    int blockno = 0;
    
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
      warn << "dhash_store failed: " << blockID << ": RPC error" << "\n";
    } else if (res->status != DHASH_OK) {
      if (res->status == DHASH_RETRY) {
	predID = res->pred->p.x;
	pred_addr = res->pred->p.r;
      } else if (res->status != DHASH_WAIT)
        warn << "dhash_store failed: " << blockID << ": "
	     << dhasherr2str(res->status) << "\n";
      if (!error)
	status = res->status;
      error = true;
    } else { 
      if ((num > nextblock) && (numblocks - num > 1)) {
	warn << "(store) FAST retransmit: " << blockID << " got " << num << " chunk " << nextblock << " of " << numblocks << " being retransmitted\n";
	clntnode->resendRPC(seqnos[nextblock]);
	//only one per fetch; finding more is too much bookkeeping
	numblocks = -1;
      }
      nextblock++;
    }

    if (npending == 0) {
      if (status == DHASH_RETRY) {
	clntnode->locations->cacheloc (predID, pred_addr, 
				       wrap (this, 
					     &dhash_store::retry_cachedloc));
      } else {
	if (error || store_type != DHASH_STORE) {
	  (*cb) (status, destID);
	  delete this;
	}
      }
    }
  }


  void store (chordID destID, chordID blockID, char *data, size_t len,
	      size_t off, size_t totsz, int num, dhash_ctype ctype, 
	      store_status store_type)
  {
    ref<dhash_storeres> res = New refcounted<dhash_storeres> (DHASH_OK);
    ref<s_dhash_insertarg> arg = New refcounted<s_dhash_insertarg> ();
    arg->v       = destID;
    arg->key     = blockID;
    arg->srcID   = clntnode->my_ID ();
    arg->data.setsize (len);
    memcpy (arg->data.base (), data, len);
    arg->offset  = off;
    arg->type    = store_type;
    arg->nonce   = nonce;
    arg->attr.size     = totsz;
    arg->last    = last;
    
    
    long rexmitid = clntnode->doRPC (destID, dhash_program_1, DHASHPROC_STORE, arg, res,
				     wrap (this, &dhash_store::finish, res, num));
    seqnos.push_back (rexmitid);
  }
  
  void retry_cachedloc (chordID id, bool ok, chordstat stat) 
  {
    if (!ok || stat) {
      warn << "challenge of " << id << " failed\n";
      (*cb) (DHASH_CHORDERR, destID);
      delete this;
    } else {
      num_retries++;
      if (num_retries > 2) {
	(*cb)(DHASH_RETRY, destID);
	delete this;
      } else {
	warn << "retrying(" << num_retries << "): dest was " 
	     << destID << " now is " << predID << "\n";
	destID = predID;
	start ();
      }
    }
  }
public:
  
  static void execute (ptr<vnode> clntnode, chordID destID, chordID blockID,
                       dhash *dh, ref<dhash_block> block, bool last,
		       cbinsert_t cb, store_status store_type = DHASH_STORE)
  {
    vNew dhash_store (clntnode, destID, blockID, dh,
	              block, store_type, last, cb);
  }
};




// ---------------------------------------------------------------------------
// DHASHCLI

 
dhashcli::dhashcli (ptr<vnode> node, dhash *dh, ptr<route_factory> r_factory,  bool do_cache) : 
  clntnode (node), 
  do_cache (do_cache),
  dh (dh),
  r_factory (r_factory)
{
  
}

void
dhashcli::retrieve (chordID blockID, bool askforlease, 
		    bool usecachedsucc, cb_ret cb)

{
  ///warn << "dhashcli::retrieve\n";

  ref<route_dhash> iterator = New refcounted<route_dhash>(r_factory, 
							  blockID,
							  dh,
							  askforlease,
							  usecachedsucc);
  
  iterator->execute (wrap (this, &dhashcli::retrieve_hop_cb, 
			   cb, blockID));
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

void
dhashcli::insert (chordID blockID, ref<dhash_block> block, 
                  bool usecachedsucc, cbinsert_t cb)
{
  lookup (blockID, usecachedsucc, 
	  wrap (this, &dhashcli::insert_lookup_cb,
		blockID, block, cb, 0));
}

void
dhashcli::insert_lookup_cb (chordID blockID, ref<dhash_block> block,
			    cbinsert_t cb, int trial,
			    dhash_stat status, chordID destID)
{
  if (status != DHASH_OK) {
    warn << "insert_lookup_cb: failure\n";
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
dhashcli::lookup (chordID blockID, bool usecachedsucc, 
		     dhashcli_lookupcb_t cb)
{
  
  if (usecachedsucc) {
    chordID x = clntnode->lookup_closestsucc (blockID);
    (*cb) (DHASH_OK, x);
  }
  else
    clntnode->find_successor (blockID,
			      wrap (this, &dhashcli::lookup_findsucc_cb,
				    blockID, cb));
}

void
dhashcli::lookup_findsucc_cb (chordID blockID, dhashcli_lookupcb_t cb,
			      chordID succID, route path, chordstat err)
{
  if (err) 
    (*cb) (DHASH_CHORDERR, 0);
  else
    (*cb) (DHASH_OK, succID);
}


