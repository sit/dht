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

  dhashcli *dhcli;
  chordID destID;
  chordID blockID;
  ptr<dhash_block> block;
  cbstore_t cb;
  dhash_ctype ctype;
  store_status store_type;

  dhash_store (dhashcli *dhcli, chordID destID, chordID blockID, 
	       ptr<dhash_block> _block, store_status store_type, cbstore_t cb)
    : npending (0), error (false), dhcli (dhcli), destID (destID), 
		 blockID (blockID), cb (cb), store_type (store_type)
  {
    ctype = dhash::block_type (_block);
    block = _block;

    size_t nstored = 0;
    while (nstored < block->len) {
      size_t chunklen = MIN (MTU, block->len - nstored);
      char  *chunkdat = &block->data[nstored];
      size_t chunkoff = nstored;
      npending++;
      dhcli->store (destID, blockID, chunkdat, chunklen, chunkoff, 
	            block->len, ctype, store_type,
		    wrap (this, &dhash_store::finish));
      nstored += chunklen;
    }
  }


  void finish (ptr<dhash_storeres> res)
  {
    ///warn << "dhash_store....finish: " << npending << "\n";
    npending--;

    if (res->status != DHASH_OK) 
      fail (dhasherr2str (res->status));

    if (npending == 0) {
      //in the gateway, the second arg is the blockID
      //here the block ID was already known to the caller
      //so return something useful: the destID
      (*cb) (error, destID);
      delete this;
    }
  }

  void fail (str errstr)
  {
    warn << "dhash_store failed: " << blockID << ": " << errstr << "\n";
    error = true;
  }

public:

  static void execute (dhashcli *dhcli, chordID destID, chordID blockID,
                       ref<dhash_block> block, cbstore_t cb, 
		       store_status store_type = DHASH_STORE)
  {
    vNew dhash_store (dhcli, destID, blockID, block, store_type, cb);
  }
};




// ---------------------------------------------------------------------------
// DHASHCLI

void
dhashcli::doRPC (chordID ID, rpc_program prog, int procno,
		 ptr<void> in, void *out, aclnt_cb cb)
{
  clntnode->doRPC (ID, prog, procno, in, out, cb);
}


void
dhashcli::retrieve (chordID blockID, bool usecachedsucc, cbretrieve_t cb)
{
  ///warn << "dhashcli::retrieve\n";
  ptr<route_dhash> iterator = 
    New refcounted<route_dhash>(clntnode->active,
				blockID,
				usecachedsucc);

  iterator->first_hop (wrap (this, &dhashcli::retrieve_hop_cb, iterator, cb));
}

void
dhashcli::retrieve_hop_cb (ptr<route_dhash> iterator, cbretrieve_t cb,
			   bool done) 
{
  if (done) {
    if (iterator->status ()) {
      (*cb) (NULL);
    } else {
      ptr<dhash_block> res = iterator->get_block ();
      cb (res); 
      cache_block (res, iterator->path (), iterator->key ());
    }
  } else 
    iterator->next_hop ();
}

void
dhashcli::cache_block (ptr<dhash_block> block, route search_path, chordID key)
{

  if (block && do_cache && 
      (dhash::block_type (block) == DHASH_CONTENTHASH)) {
    unsigned int path_size = search_path.size ();
    if (path_size > 1) {
      chordID cache_dest = search_path[path_size - 2];
      dhash_store::execute (this, cache_dest, key, block, 
			    wrap (this, &dhashcli::finish_cache),
			    DHASH_CACHE);
    }
    
  }
}

void
dhashcli::finish_cache (bool error, chordID dest)
{
  if (error)
    warn << "error caching block\n";
}

//use this version if you already know where the block is (and guessing
// that you have the successor cached won't work)
void
dhashcli::retrieve (chordID source, chordID blockID, cbretrieve_t cb)
{
  ptr<route_dhash> iterator = 
    New refcounted<route_dhash>(clntnode->active,
				blockID,
				source); //this is the "guess"

  iterator->first_hop (wrap (this, &dhashcli::retrieve_with_source_cb, 
			     iterator, cb));  
}

void
dhashcli::retrieve_with_source_cb (ptr<route_dhash> iterator,
				   cbretrieve_t cb, bool done)
{
  if (done) {
    if (iterator->status ()) 
      (*cb) (NULL);
    else 
      cb (iterator->get_block ()); 
  } else 
    iterator->next_hop ();
}

void
dhashcli::insert (chordID blockID, ref<dhash_block> block, 
                  bool usecachedsucc, cbinsert_t cb)
{
  //  dhash_insert::execute (this, blockID, block, usecachedsucc, cb);
  lookup (blockID, usecachedsucc, 
	  wrap (this, &dhashcli::insert_lookup_cb,
		blockID, block, cb));
}

void
dhashcli::insert_lookup_cb (chordID blockID, ref<dhash_block> block,
			    cbinsert_t cb, dhash_stat status, chordID destID)
{
  if (status != DHASH_OK)
    (*cb) (true, bigint(0)); // failure
  else 
    dhash_store::execute (this, destID, blockID, block,  cb);
}

//like insert, but doesn't do lookup. used by transfer_key
void
dhashcli::storeblock (chordID dest, chordID ID, ref<dhash_block> block, cbstore_t cb, store_status stat)
{
  dhash_store::execute (this, dest, ID, block, cb, stat);
}

void
dhashcli::store (chordID destID, chordID blockID, char *data, size_t len,
                 size_t off, size_t totsz, dhash_ctype ctype, 
		 store_status store_type,
		 dhashcli_storecb_t cb)
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
  doRPC (destID, dhash_program_1, DHASHPROC_STORE, arg, res,
	 wrap (this, &dhashcli::store_cb, cb, res));
}

void
dhashcli::store_cb (dhashcli_storecb_t cb, ref<dhash_storeres> res,
		    clnt_stat err)
{
  // XXX is this ok?
  if (err) {
    warn << "dhashcli::store_cb: err " << err << "\n";
    res->set_status (DHASH_RPCERR);
  }

  (*cb) (res);    
}



void
dhashcli::lookup (chordID blockID, bool usecachedsucc, dhashcli_lookupcb_t cb)
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



void
dhashcli::lookup_route (chordID blockID, bool usecachedsucc, dhashcli_routecb_t cb)
{
  if (usecachedsucc) {
    chordID x = clntnode->lookup_closestsucc (blockID);
    route e_path;
    (*cb) (DHASH_OK, x, e_path);
  }
  clntnode->find_successor (blockID,
			    wrap (this, &dhashcli::lookup_findsucc_route_cb,
				  blockID, cb));
}

void
dhashcli::lookup_findsucc_route_cb (chordID blockID, dhashcli_routecb_t cb,
				    chordID succID, route path, chordstat err)
{
  if (err) {
    route e_path;
    (*cb) (DHASH_CHORDERR, 0, e_path);
  }  else
    (*cb) (DHASH_OK, succID, path);
}
