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
#include "arpc.h"
#ifdef DMALLOC
#include "dmalloc.h"
#endif


// ------------------------------------------------------------------------
// DHASHGATEWAY

dhashgateway::dhashgateway (ptr<axprt_stream> x, 
			    ptr<chord> node,
			    dhash *dh,
			    ptr<route_factory> f,
			    bool do_cache)
{
  clntsrv = asrv::alloc (x, dhashgateway_program_1, 
			 wrap (this, &dhashgateway::dispatch));
  clntnode = node;
  this->dh = dh;
  dhcli = New refcounted<dhashcli>(clntnode->active, dh, f, do_cache);
}


void
dhashgateway::dispatch (svccb *sbp)
{
  if (!sbp)
    return;

  assert (clntnode);

  switch (sbp->proc ()) {
  case DHASHPROC_NULL:
    sbp->reply (NULL);
    return;

  case DHASHPROC_INSERT:
    {
      dhash_insert_arg *arg = sbp->template getarg<dhash_insert_arg> ();

      ref<dhash_block> block =
	New refcounted<dhash_block> (arg->block.base (), arg->block.size ());
      dhcli->insert (arg->blockID, block, arg->usecachedsucc,
	             wrap (this, &dhashgateway::insert_cb, sbp));

    }
    break;
    
  case DHASHPROC_RETRIEVE:
    {
      dhash_retrieve_arg *arg = sbp->template getarg<dhash_retrieve_arg> ();
      dhcli->retrieve (arg->blockID,
	               arg->askforlease,
	               arg->usecachedsucc,
	               wrap (this, &dhashgateway::retrieve_cb, sbp));
    }
    break;
    
    //XXX DHASHPROC_ACTIVE messes up the back-call scheme. Disabled. --FED

  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

void
dhashgateway::insert_cb (svccb *sbp, dhash_stat status, chordID destID)
{
  dhash_insert_res res (status);
  if (status == DHASH_OK)
    res.resok->destID = destID;
  sbp->reply (&res);
}


void
dhashgateway::retrieve_cb (svccb *sbp, ptr<dhash_block> block)
{
  ///warn << "dhashgateway::retrieve_cb\n";

  dhash_retrieve_res res (DHASH_OK);

  if (!block)
    res.set_status (DHASH_NOENT);
  else {
    res.resok->block.setsize (block->len);
    res.resok->hops = block->hops % 100;
    res.resok->errors = block->hops / 100;
    res.resok->lease = block->lease;
    memcpy (res.resok->block.base (), block->data, block->len);
  }
  sbp->reply (&res);
}



