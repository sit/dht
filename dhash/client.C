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
//     - XXX don't really handle RETRY/FAILURE/RACE conditions..

class dhash_store {
protected:
  uint npending;
  bool error;
  dhash_stat status;

  chordID destID;
  chordID blockID;
  ptr<dhash_block> block;
  cbinsert_t cb;
  dhash_ctype ctype;
  store_status store_type;
  ptr<chord> clntnode;

  dhash_store (ptr<chord> clntnode, chordID destID, chordID blockID, 
	       ptr<dhash_block> _block, store_status store_type, cbinsert_t cb)
    : npending (0), error (false), status (DHASH_OK), destID (destID), 
		 blockID (blockID), cb (cb), store_type (store_type),
		 clntnode (clntnode)
  {
    ctype = block_type (_block);
    block = _block;

    size_t nstored = 0;
    while (nstored < block->len) {
      size_t chunklen = MIN (MTU, block->len - nstored);
      char  *chunkdat = &block->data[nstored];
      size_t chunkoff = nstored;
      npending++;
      store (destID, blockID, chunkdat, chunklen, chunkoff, 
	     block->len, ctype, store_type);
      nstored += chunklen;
    }
  }
  

  void finish (ptr<dhash_storeres> res, clnt_stat err)
  {
    ///warn << "dhash_store....finish: " << npending << "\n";
    npending--;

    //XXX sure res isn't null?
    if (res->status != DHASH_OK) {
      if (res->status != DHASH_WAIT)
        warn << "dhash_store failed: " << blockID << ": "
	     << dhasherr2str(res->status) << "\n";
      if (!error)
	status = res->status;
      error = true;
    }

    if (npending == 0) {
      //in the gateway, the second arg is the blockID
      //here the block ID was already known to the caller
      //so return something useful: the destID
      (*cb) (status, destID);
      delete this;
    }
  }


  void store (chordID destID, chordID blockID, char *data, size_t len,
	      size_t off, size_t totsz, dhash_ctype ctype, 
	      store_status store_type)
  {
    ref<dhash_storeres> res = New refcounted<dhash_storeres> (DHASH_OK);
    ref<s_dhash_insertarg> arg = New refcounted<s_dhash_insertarg> ();
    arg->v       = destID;
    arg->key     = blockID;
    arg->data.setsize (len);
    memcpy (arg->data.base (), data, len);
    arg->offset  = off;
    arg->type    = store_type;
    arg->attr.size     = totsz;
    
    ///warn << "XXXXXX dhashcli::store ==> store_cb\n";
    clntnode->doRPC (destID, dhash_program_1, DHASHPROC_STORE, arg, res,
		     wrap (this, &dhash_store::finish, res));
  }
  
  void fail (str errstr)
  {
    warn << "dhash_store failed: " << blockID << ": " << errstr << "\n";
    error = true;
  }
  
public:
  
  static void execute (ptr<chord> clntnode, chordID destID, chordID blockID,
                       ref<dhash_block> block, cbinsert_t cb, 
		       store_status store_type = DHASH_STORE)
  {
    vNew dhash_store (clntnode, destID, blockID, block, store_type, cb);
  }
};




// ---------------------------------------------------------------------------
// DHASHCLI

 
dhashcli::dhashcli (ptr<chord> node, dhash *dh, ptr<route_factory> r_factory,  bool do_cache) : 
  clntnode (node), 
  do_cache (do_cache),
  dh (dh),
  r_factory (r_factory)
{
  
}

void
dhashcli::retrieve (chordID blockID, bool askforlease, 
		       bool usecachedsucc, cbretrieve_t cb)

{
  ///warn << "dhashcli::retrieve\n";

  route_dhash *iterator = New route_dhash(r_factory, 
					  blockID,
					  dh,
					  askforlease,
					  usecachedsucc);
  

  iterator->execute (wrap (this, &dhashcli::retrieve_hop_cb, 
			   iterator, cb, blockID));
}

void
dhashcli::retrieve_hop_cb (route_dhash *iterator, cbretrieve_t cb, chordID key,
			   dhash_stat status, 
			   ptr<dhash_block> blk, 
			   route path) 
{
  if (status) {
    warn << "iterator exiting w/ status\n";
    (*cb) (NULL);
  } else {
    cb (blk); 
    cache_block (blk, path, key);
  }
  delete iterator;
}

void
dhashcli::cache_block (ptr<dhash_block> block, route search_path, chordID key)
{

  if (block && do_cache && 
      (block_type (block) == DHASH_CONTENTHASH)) {
    unsigned int path_size = search_path.size ();
    if (path_size > 1) {
      chordID cache_dest = search_path[path_size - 2];
      dhash_store::execute (clntnode, cache_dest, key, block, 
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
dhashcli::retrieve (chordID source, chordID blockID, cbretrieve_t cb)
{
  route_dhash *iterator = New route_dhash(r_factory, 
					  blockID, dh);

  iterator->execute (wrap (this, &dhashcli::retrieve_with_source_cb, 
			   iterator, cb), 
		     source);  
}

void
dhashcli::retrieve_with_source_cb (route_dhash *iterator, cbretrieve_t cb, 
				   dhash_stat status, 
				   ptr<dhash_block> block, route path)
{
  if (status) 
    (*cb) (NULL);
  else 
    cb (block); 
  delete iterator;
}

void
dhashcli::insert (chordID blockID, ref<dhash_block> block, 
                  bool usecachedsucc, cbinsert_t cb)
{
  lookup (blockID, usecachedsucc, 
	  wrap (this, &dhashcli::insert_lookup_cb,
		blockID, block, cb));
}

void
dhashcli::insert_lookup_cb (chordID blockID, ref<dhash_block> block,
			    cbinsert_t cb, dhash_stat status, chordID destID)
{
  if (status != DHASH_OK) {
    warn << "insert_lookup_cb: failure\n";
    (*cb) (status, bigint(0)); // failure
  }else 
    dhash_store::execute (clntnode, destID, blockID, block, cb);
}

//like insert, but doesn't do lookup. used by transfer_key
void

dhashcli::storeblock (chordID dest, chordID ID, ref<dhash_block> block, 
			 cbinsert_t cb, store_status stat)
{
  dhash_store::execute (clntnode, dest, ID, block, cb, stat);
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


