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
// DHASH_RETRIEVE
//    - retrieves a given blockID
//    - Handles the lookup of the chord node holding the block
//      and various RACE/FAILURE conditions, by way of dhcli->lookup_iter.
//    - XXX these failure conditions need to be code reviewed.  
//          Can a simpler algorithm like this work:
//          while (nodeID = lookup (blockID))
//             <err, path, block> = retrieve (nodeID, blockID)
//             if     (err == NO_SUCH_BLOCK) 
//                  return NO_SUCH_BLOCK
//             elseif (err == RETRY)
//                  notify (path.penultimate, path.back); // and try around..
//             else
//                   return DHASH_CHORDERR;
//
//    - XXX err....the above isn't really correct..


class dhash_retrieve {
protected:
  dhashcli *dhcli;
  uint npending;
  bool error;
  bigint key;
  cbretrieve_t cb;  
  ptr<dhash_block> block;


  dhash_retrieve (dhashcli *dhcli, bigint key, cbretrieve_t cb)
    : dhcli (dhcli), npending (0), error (false), key (key), cb (cb),
      block (NULL)
  {
    npending++;
    dhcli->lookup_iter (key, 0, MTU, wrap (this, &dhash_retrieve::first_chunk_cb));
  }


  void first_chunk_cb (ptr<dhash_res> res)
  {
    ///warn << "dhash_retrieve::first_chunk_cb\n";

    if (res->status == DHASH_OK) {
      size_t totsz     = res->resok->attr.size;
      size_t nread     = res->resok->res.size ();
      chordID sourceID = res->resok->source;
      block            = New refcounted<dhash_block> ((char *)NULL, totsz);
      while (nread < totsz) {
	uint32 offset = nread;
	uint32 length = MIN (MTU, totsz - nread);
	npending++;
	dhcli->lookup_iter (sourceID, key, offset, length,
			    wrap (this, &dhash_retrieve::finish));
	nread += length;
      }
    }

    finish (res);
  }
    

  void finish (ptr<dhash_res> res)
  {
    ///warn << "dhash_retrieve::finish(), npending=" << npending << "\n";
    npending--;

    if (res->status != DHASH_OK) 
      fail (dhasherr2str (res->status));
    else {
      uint32 off = res->resok->offset;
      uint32 len = res->resok->res.size ();
      if (off + len > block->len)
	fail (strbuf ("bad fragment: off %d, len %d, block %d", off, len, block->len));
      else
	memcpy (&block->data[off], res->resok->res.base (), len);
    }

    if (npending == 0) {
      if (error)
	block = NULL;
      (*cb) (block);
      delete this;
    }
  }

  void fail (str errstr)
  {
    ///warn << "dhash_retrieve failed: " << key << ": " << errstr << "\n";
    error = true;
  }


public:
  static void execute (dhashcli *dhcli, bigint key, cbretrieve_t cb)
  {
    vNew dhash_retrieve (dhcli, key, cb);
  }
};



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
  ref<dhash_block> block;
  dhash_ctype ctype;
  cbstore_t cb;

  dhash_store (dhashcli *dhcli, chordID destID, chordID blockID, ref<dhash_block> block, dhash_ctype ctype, cbstore_t cb)
    : npending (0), error (false), dhcli (dhcli), destID (destID), blockID (blockID), block (block), 
		 ctype (ctype), cb (cb)
  {
    size_t nstored = 0;
    while (nstored < block->len) {
      size_t chunklen = MIN (MTU, block->len - nstored);
      char  *chunkdat = &block->data[nstored];
      size_t chunkoff = nstored;
      npending++;
      dhcli->store (destID, blockID, chunkdat, chunklen, chunkoff, block->len, ctype,
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
      ///warn << "dhash_store....all done and calling back\n";
      (*cb) (error, blockID);
      delete this;
    }
  }

  void fail (str errstr)
  {
    warn << "dhash_store failed: " << blockID << ": " << errstr << "\n";
    error = true;
  }

public:

  static void execute (dhashcli *dhcli, chordID destID, chordID blockID, ref<dhash_block> block, 
		       dhash_ctype ctype, cbstore_t cb)
  {
    vNew dhash_store (dhcli, destID, blockID, block, ctype, cb);
  }
};




// ---------------------------------------------------------------------------
// DHASH_INSERT
//     - store a give block of data into DHASH.
//     - The location of the ring  
//     - XXX don't really handle RETRY/FAILURE/RACE conditions..


class dhash_insert {
protected:
  dhashcli *dhcli;
  chordID blockID ;
  ref<dhash_block> block;
  dhash_ctype ctype;
  cbinsert_t cb;


  dhash_insert (dhashcli *dhcli, chordID blockID, ref<dhash_block> block, dhash_ctype ctype, cbinsert_t cb)
    : dhcli (dhcli), blockID (blockID), block (block), ctype (ctype), cb (cb)
  {
    dhcli->lookup (blockID, wrap (this, &dhash_insert::lookup_cb));
  }

  void lookup_cb (dhash_stat status, chordID destID)
  {
    //    warn << "store " << blockID << " at " << destID << "\n";

    if (status != DHASH_OK)
      (*cb) (true, blockID); // failure
    else
      dhash_store::execute (dhcli, destID, blockID, block, ctype, cb);
    delete this;
  }

public:
  static void execute (dhashcli *dhcli, chordID blockID, ref<dhash_block> block, dhash_ctype ctype, cbinsert_t cb)
  {
    vNew dhash_insert (dhcli, blockID, block, ctype, cb);
  }
};



// ---------------------------------------------------------------------------
// UTIL ROUTINES

static void
iterres2res (dhash_fetchiter_res *ires, dhash_res *res) 
{
    res->resok->offset = ires->compl_res->offset;
    res->resok->attr = ires->compl_res->attr;
    res->resok->source = ires->compl_res->source;
    res->resok->res = ires->compl_res->res;
    res->resok->hops = 0;
}


static bool
straddled (route path, chordID &k)
{
  int n = path.size ();
  if (n < 2) return false;
  chordID prev = path[n-1];
  chordID pprev = path[n-2];
  return between (pprev, prev, k);
}



// ---------------------------------------------------------------------------
// DHASHCLI

void
dhashcli::doRPC (chordID ID, rpc_program prog, int procno,
		 ptr<void> in, void *out, aclnt_cb cb)
{
  clntnode->doRPC (ID, prog, procno, in, out, cb);
}


void
dhashcli::retrieve (chordID blockID, cbretrieve_t cb)
{
  ///warn << "dhashcli::retrieve\n";
  dhash_retrieve::execute (this, blockID, cb);
}


void
dhashcli::insert (chordID blockID, ref<dhash_block> block, dhash_ctype ctype, cbinsert_t cb)
{
  dhash_insert::execute (this, blockID, block, ctype, cb);
}


void
dhashcli::lookup_iter (chordID sourceID, chordID blockID, uint32 off, uint32 len, dhashcli_lookup_itercb_t cb)
{
  warnt ("DHASH: lookup_iter");
  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
  arg->v     = sourceID;
  arg->key   = blockID;
  arg->start = off;
  arg->len   = len;

  ptr<dhash_fetchiter_res> res = New refcounted<dhash_fetchiter_res> (DHASH_OK);
  doRPC (sourceID, dhash_program_1, DHASHPROC_FETCHITER, arg, res,
    	 wrap (this, &dhashcli::lookup_iter_with_source_cb, res, cb));
}

void
dhashcli::lookup_iter_with_source_cb (ptr<dhash_fetchiter_res> fres,
				      dhashcli_lookup_itercb_t cb, clnt_stat err)
{
  ///warn << "dhashcli::lookup_iter_with_source_cb\n";

  ptr<dhash_res> res = New refcounted<dhash_res> (DHASH_OK);
  if (err || fres->status != DHASH_COMPLETE)
    res->set_status (DHASH_RPCERR);
  else 
    iterres2res(fres, res);
  (*cb) (res);
}




void
dhashcli::lookup_iter (chordID blockID, uint32 off, uint32 len, dhashcli_lookup_itercb_t cb)
{
  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
  arg->key   = blockID;
  arg->start = off;
  arg->len   = len;
  arg->v     = clntnode->clnt_ID ();
  
  route path;
  path.push_back (clntnode->clnt_ID ());
  ref<dhash_fetchiter_res> res = New refcounted <dhash_fetchiter_res> (DHASH_CONTINUE);
  doRPC (path[0], dhash_program_1, DHASHPROC_FETCHITER, arg, res, 
	 wrap (this, &dhashcli::lookup_iter_cb, blockID, cb, res, path, 0));
}




void 
dhashcli::lookup_iter_cb (chordID blockID, dhashcli_lookup_itercb_t cb,
			  ref<dhash_fetchiter_res> res,
			  route path,
			  int nerror,
			  clnt_stat err)
{
  ///warn << "dhashcli::lookup_iter_cb\n";

  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
  arg->key   = blockID;
  arg->start = 0;
  arg->len   = MTU;

  if (err) {
    /* CASE I */
    chordID last;
    chordID plast;
    nerror++;
    if (path.size () > 0)
      last = path.pop_back ();
    if (path.size () > 0) {
      plast = path.back ();
      clntnode->alert (plast, last);
    } else {
#ifdef FINGERS
      plast = clntnode->lookup_closestpred (blockID);
#else
      plast = clntnode->lookup_closestsucc (blockID);
#endif
      path.push_back (plast);
    }
    if (plast == clntnode->clnt_ID ()) {
      (*cb) (New refcounted<dhash_res> (DHASH_NOENT));
    } else {
      // XXX Assumes an in-order RPC transport, otherwise retry
      //     might reach prev before alert can update tables.
      //     Warning.....................the transport is UDP!!
      arg->v = plast;
      ref<dhash_fetchiter_res> nres = New refcounted<dhash_fetchiter_res> (DHASH_CONTINUE);
      doRPC (plast, dhash_program_1, DHASHPROC_FETCHITER, arg, nres,
	     wrap(this, &dhashcli::lookup_iter_cb, blockID, cb, nres, path, nerror));
    }
  } else if (res->status == DHASH_COMPLETE) {
    /* CASE II */

#if 0
#error this should happen somewhere else...
    // a zero size just checks if the block exists, ie. no download occurs
    if (res->compl_res->res.size() > 0) {
      save_chunk (blockID, res, path);
      cache_on_path (blockID, path);
    }
#endif

    ref<dhash_res> fres = New refcounted<dhash_res> (DHASH_OK);
    fres->resok->res = res->compl_res->res;
    fres->resok->offset = res->compl_res->offset;
    fres->resok->attr = res->compl_res->attr;
    fres->resok->hops = path.size()-1 + nerror*100;
    fres->resok->source = path.back ();
    (*cb) (fres);
  } else if (res->status == DHASH_CONTINUE) {
    chordID next = res->cont_res->next.x;
    chordID prev = path.back ();
    if ((next == prev) || (straddled (path, blockID))) {
      (*cb) (New refcounted<dhash_res> (DHASH_NOENT));
    } else {
      clntnode->cacheloc
	(next, res->cont_res->next.r,
	 wrap (this, &dhashcli::lookup_iter_chalok_cb, arg, cb, path, nerror));
    }
  } else {
    /* the last node queried was responbile but doesn't have it */
    (*cb) (New refcounted<dhash_res> (DHASH_NOENT));
  }
}

void dhashcli::lookup_iter_chalok_cb (ptr<s_dhash_fetch_arg> arg,
				      dhashcli_lookup_itercb_t cb,
				      route path,
				      int nerror,
				      chordID next, bool ok, chordstat s)
{
  if (ok && s == CHORD_OK) {
    path.push_back (next);
    assert (path.size () < 1000);

    arg->v = next;
    ptr<dhash_fetchiter_res> nres = New refcounted<dhash_fetchiter_res> (DHASH_CONTINUE);
    doRPC (next, dhash_program_1, DHASHPROC_FETCHITER, arg, nres,
	   wrap (this, &dhashcli::lookup_iter_cb,
		 arg->key, cb, nres, path, nerror));
  }
}


void
dhashcli::store (chordID destID, chordID blockID, char *data, size_t len, size_t off, size_t totsz, 
		 dhash_ctype ctype, dhashcli_storecb_t cb)
{
  ref<dhash_storeres> res = New refcounted<dhash_storeres> (DHASH_OK);
  ref<s_dhash_insertarg> arg = New refcounted<s_dhash_insertarg> ();
  arg->v       = destID;
  arg->key     = blockID;
  arg->data.setsize (len);
  memcpy (arg->data.base (), data, len);
  arg->offset  = off;
  arg->type    = DHASH_STORE;  // XXX HACK, FIXME
  arg->attr.size     = totsz;
  arg->attr.ctype    = ctype;

  ///warn << "XXXXXX dhashcli::store ==> store_cb\n";
  doRPC (destID, dhash_program_1, DHASHPROC_STORE, arg, res,
	 wrap (this, &dhashcli::store_cb, cb, res));
}

void
dhashcli::store_cb (dhashcli_storecb_t cb, ref<dhash_storeres> res,
		    clnt_stat err)
{
  ///warn << "XXXXXX dhashcli::store_cb\n";



  // XXX is this ok?
  if (err) {
    warn << "dhashcli::store_cb: err " << err << "\n";
    res->set_status (DHASH_RPCERR);
  }

  (*cb) (res);    
}



void
dhashcli::lookup (chordID blockID, dhashcli_lookupcb_t cb)
{
  clntnode->find_successor (blockID,
			    wrap (this, &dhashcli::lookup_findsucc_cb, blockID, cb));
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



