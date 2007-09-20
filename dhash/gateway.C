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
#include <chord_types.h>
#include <dhashgateway_prot.h>
#include <location.h>

#include "dhash_common.h"
#include "dhash.h"
#include "dhashcli.h"
#include "dhashgateway.h"

#include "arpc.h"
#ifdef DMALLOC
#include "dmalloc.h"
#endif


// ------------------------------------------------------------------------
// DHASHGATEWAY

dhashgateway::dhashgateway (ptr<axprt_stream> x, 
			    ptr<chord> node,
			    ref<dhash> dh)
{
  clntsrv = asrv::alloc (x, dhashgateway_program_1,
	                 wrap (mkref (this), &dhashgateway::dispatch));
  dhcli = New refcounted<dhashcli> (node->get_vnode (0), dh);
}

dhashgateway::~dhashgateway ()
{
}

void
dhashgateway::dispatch (svccb *sbp)
{
  if (!sbp) {
    // setting clntsrv to 0 removes the last reference to this gateway
    // object, stored in the asrv object's callback.
    clntsrv = 0;
    return;
  }

  switch (sbp->proc ()) {
  case DHASHPROC_NULL:
    sbp->reply (NULL);
    return;

  case DHASHPROC_INSERT:
    {
      dhash_insert_arg *arg = sbp->Xtmpl getarg<dhash_insert_arg> ();

      ref<dhash_block> block =
	New refcounted<dhash_block> (arg->block.base (), arg->block.size (), arg->ctype);
      block->ID = arg->blockID;

      block->expiration = 0;
      if (arg->options & DHASHCLIENT_EXPIRATION_SUPPLIED)
	block->expiration = arg->expiration;

      ptr<chordID> guess = NULL;
      if (arg->options & DHASHCLIENT_GUESS_SUPPLIED)
	guess = New refcounted<chordID> (arg->guess);

      dhcli->insert (block,
	             wrap (mkref (this), &dhashgateway::insert_cb, sbp),
		     arg->options, guess);
    }
    break;
    
  case DHASHPROC_RETRIEVE:
    {
      dhash_retrieve_arg *arg = sbp->Xtmpl getarg<dhash_retrieve_arg> ();
        
      ptr<chordID> guess = NULL;
      if (arg->options & DHASHCLIENT_GUESS_SUPPLIED)
	guess = New refcounted<chordID> (arg->guess);

      dhcli->retrieve
	(blockID (arg->blockID, arg->ctype),
	 wrap (mkref (this), &dhashgateway::retrieve_cb, sbp),
	 arg->options, guess);
    }
    break;

  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

void
dhashgateway::insert_cb (svccb *sbp, dhash_stat status, vec<chordID> path)
{
  dhash_insert_res res (status);
  //warn << "dhashgateway::insert_cb dhash_stat = " << status << "\n";
  if (status == DHASH_OK) {
    //warn << "dhashgateway::insert_cb Insert succeeded\n";
    res.resok->path.setsize (path.size ());
    for (unsigned int i = 0; i < path.size (); i++)
      res.resok->path[i] = path[i];
  }
  sbp->reply (&res);
}


void
dhashgateway::retrieve_cb (svccb *sbp, dhash_stat stat,
                           ptr<dhash_block> block, route path)
{
  dhash_retrieve_res res (DHASH_OK);
  if (!block)
    res.set_status (stat);
  else {
    res.resok->block.setsize (block->data.len ());
    res.resok->ctype = block->ctype;
    res.resok->expiration = block->expiration;
    res.resok->hops = block->hops;
    res.resok->errors = block->errors;
    res.resok->retries = block->retries;
    res.resok->path.setsize (path.size ());
    for (u_int i = 0; i < path.size (); i++)
      res.resok->path[i] = path[i]->id ();
    res.resok->times.setsize (block->times.size ());
    for (u_int i = 0; i < block->times.size (); i++)
      res.resok->times[i] = block->times[i];
    memcpy (res.resok->block.base (), block->data.cstr (), block->data.len ());
  }
  sbp->reply (&res);
}

