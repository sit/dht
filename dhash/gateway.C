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

dhashgateway::dhashgateway (ptr<axprt_stream> x, ptr<chord> node)
{
  useproxy = false;
  clntsrv = asrv::alloc (x, dhashgateway_program_1,
	                 wrap (mkref (this), &dhashgateway::dispatch));
  dhcli = New refcounted<dhashcli> (node->active);
}

dhashgateway::dhashgateway (ptr<axprt_stream> x, ptr<chord> node,
                            str host, int port)
{
  useproxy = true;
  proxyclnt = 0;
  proxyhost = host;
  proxyport = port;
  tcpconnect (proxyhost, proxyport,
              wrap (mkref (this), &dhashgateway::proxy_connected, x, node));
}

void
dhashgateway::proxy_connected (ptr<axprt_stream> x, ptr<chord> node, int fd)
{
  if (fd < 0) {
    warn << "cannot connect to proxy "
         << proxyhost << ":" << proxyport << ", skip proxying\n";
    proxyclnt = 0;
  }
  else {
    ref<axprt_stream> x = axprt_stream::alloc (fd, 1024*1025);
    proxyclnt = aclnt::alloc (x, dhashgateway_program_1);
  }

  clntsrv = asrv::alloc (x, dhashgateway_program_1,
	                 wrap (mkref (this), &dhashgateway::dispatch));
  dhcli = New refcounted<dhashcli> (node->active);
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
      dhash_insert_arg *arg = sbp->template getarg<dhash_insert_arg> ();

      ref<dhash_block> block =
	New refcounted<dhash_block> (arg->block.base (), arg->len, arg->ctype);
      block->ID = arg->blockID;

      if ((arg->options & DHASHCLIENT_USE_CACHE) ||
	  (useproxy && proxyclnt == 0))
        dhcli->insert_to_cache
	  (block, wrap (mkref (this), &dhashgateway::insert_cb, sbp, true));
      
      else if (useproxy) {
	int options = arg->options;
	arg->options =
	  (arg->options & (~(DHASHCLIENT_USE_CACHE | DHASHCLIENT_CACHE)));
  
	ptr<dhash_insert_res> res = New refcounted<dhash_insert_res> ();
	proxyclnt->call
	  (DHASHPROC_INSERT, arg, res,
	   wrap (mkref (this), &dhashgateway::proxy_insert_cb,
	         options, sbp, res));
      }

      else {
        ptr<chordID> guess = NULL;
        if (arg->options & DHASHCLIENT_GUESS_SUPPLIED) 
	  guess = New refcounted<chordID> (arg->guess);
	 
        dhcli->insert
	  (block, wrap (mkref (this), &dhashgateway::insert_cb, sbp, false),
	   arg->options, guess);
      }
    }
    break;
    
  case DHASHPROC_RETRIEVE:
    {
      dhash_retrieve_arg *arg = sbp->template getarg<dhash_retrieve_arg> ();
        
      ptr<chordID> guess = NULL;
      
      if (arg->options & DHASHCLIENT_GUESS_SUPPLIED)
	guess = New refcounted<chordID> (arg->guess);

      if ((arg->options & DHASHCLIENT_USE_CACHE) ||
	  (useproxy && proxyclnt == 0))
        dhcli->retrieve_from_cache
	  (blockID (arg->blockID, arg->ctype, DHASH_BLOCK),
	   wrap (mkref (this), &dhashgateway::retrieve_cache_cb, sbp));

      else if (useproxy) {
	int options = arg->options;
	arg->options =
	  (arg->options & (~(DHASHCLIENT_USE_CACHE | DHASHCLIENT_CACHE)));
  
	ptr<dhash_retrieve_res> res = New refcounted<dhash_retrieve_res> ();
	proxyclnt->call
	  (DHASHPROC_RETRIEVE, arg, res,
	   wrap (mkref (this), &dhashgateway::proxy_retrieve_cb,
	         options, sbp, res));
      }

      else
        dhcli->retrieve
	  (blockID (arg->blockID, arg->ctype, DHASH_BLOCK),
	   wrap (mkref (this), &dhashgateway::retrieve_cb, sbp),
	   arg->options, guess);
    }
    break;
    
    //XXX DHASHPROC_ACTIVE messes up the back-call scheme. Disabled. --FED

  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

void
dhashgateway::insert_cache_cb (dhash_stat status, vec<chordID> path)
{
  if (status)
    warn << "insert into cache failed " << dhasherr2str (status) << "\n";
}

void
dhashgateway::proxy_insert_cb (int options, svccb *sbp,
                               ptr<dhash_insert_res> res, clnt_stat err)
{
  if (err) {
    dhash_insert_res r (DHASH_RPCERR);
    insert_cb_common (sbp, false, &r);
  }
  else {
    dhash_insert_arg *arg = sbp->template getarg<dhash_insert_arg> ();
    arg->options = options;
    insert_cb_common (sbp, false, res);
  }
}

void
dhashgateway::insert_cb (svccb *sbp, bool tocache,
                         dhash_stat status, vec<chordID> path)
{
  dhash_insert_res res (status);
  if (status == DHASH_OK) {
    res.resok->path.setsize (path.size ());
    for (unsigned int i = 0; i < path.size (); i++)
      res.resok->path[i] = path[i];
  }
  insert_cb_common (sbp, tocache, &res);
}

void
dhashgateway::insert_cb_common (svccb *sbp, bool tocache,
                                dhash_insert_res *res)
{
  // this must be before sbp->reply, otherwise sbp object is not
  // guaranteed to be around
  ptr<dhash_block> block = 0;
  dhash_insert_arg *arg = sbp->template getarg<dhash_insert_arg> ();
  if (!tocache && (arg->options & DHASHCLIENT_CACHE)) {
    block = New refcounted<dhash_block>
      (arg->block.base (), arg->len, arg->ctype);
    block->ID = arg->blockID;
  }

  sbp->reply (res);

  if (block)
    dhcli->insert_to_cache
      (block, wrap (mkref (this), &dhashgateway::insert_cache_cb));
}

#define SET_RETRIEVE_REPLY \
  if (!block) \
    res.set_status (stat); \
  else { \
    res.resok->block.setsize (block->len); \
    res.resok->ctype = block->ctype; \
    res.resok->len = block->len; \
    res.resok->hops = block->hops; \
    res.resok->errors = block->errors; \
    res.resok->retries = block->retries; \
    res.resok->path.setsize (path.size ()); \
    for (u_int i = 0; i < path.size (); i++) \
      res.resok->path[i] = path[i]->id (); \
    res.resok->times.setsize (block->times.size ()); \
    for (u_int i = 0; i < block->times.size (); i++) \
      res.resok->times[i] = block->times[i]; \
    memcpy (res.resok->block.base (), block->data, block->len); \
  }

void
dhashgateway::proxy_retrieve_cb (int options, svccb *sbp,
                                 ptr<dhash_retrieve_res> res, clnt_stat err)
{
  if (err) {
    dhash_retrieve_res r (DHASH_RPCERR);
    retrieve_cb_common (sbp, &r);
  }
  else {
    dhash_retrieve_arg *arg = sbp->template getarg<dhash_retrieve_arg> ();
    arg->options = options;
    retrieve_cb_common (sbp, res);
  }
}

void
dhashgateway::retrieve_cb (svccb *sbp, dhash_stat stat,
                           ptr<dhash_block> block, route path)
{
  dhash_retrieve_res res (DHASH_OK);
  SET_RETRIEVE_REPLY;
  retrieve_cb_common (sbp, &res);
}

void
dhashgateway::retrieve_cb_common (svccb *sbp, dhash_retrieve_res *res)
{
  // this must be before sbp->reply, otherwise sbp object is not
  // guaranteed to be around
  ptr<dhash_block> nb = 0;
  dhash_retrieve_arg *arg = sbp->template getarg<dhash_retrieve_arg> ();
  if (res->status == DHASH_OK && (arg->options & DHASHCLIENT_CACHE)) {
    nb = New refcounted<dhash_block>
      (res->resok->block.base (), res->resok->len, res->resok->ctype);
    nb->ID = arg->blockID;
  }

  sbp->reply (res);

  if (nb)
    dhcli->insert_to_cache
      (nb, wrap (mkref (this), &dhashgateway::insert_cache_cb));
}

void
dhashgateway::retrieve_cache_cb (svccb *sbp, ptr<dhash_block> block)
{
  if (block) {
    dhash_retrieve_res res (DHASH_OK);
    route path;
    dhash_stat stat = DHASH_OK;
    SET_RETRIEVE_REPLY;
    sbp->reply (&res);
  }

  else if (useproxy) {
    if (proxyclnt == 0) {
      dhash_retrieve_res res (DHASH_NOENT);
      route path;
      dhash_stat stat = DHASH_NOENT;
      SET_RETRIEVE_REPLY;
      sbp->reply (&res);
    }
    else {
      dhash_retrieve_arg *arg = sbp->template getarg<dhash_retrieve_arg> ();
      int options = arg->options;
      arg->options =
        (arg->options & (~(DHASHCLIENT_USE_CACHE | DHASHCLIENT_CACHE)));
  
      ptr<dhash_retrieve_res> res = New refcounted<dhash_retrieve_res> ();
      proxyclnt->call
        (DHASHPROC_RETRIEVE, arg, res,
         wrap (mkref (this), &dhashgateway::proxy_retrieve_cb,
	       options, sbp, res));
    }
  }

  else {
    dhash_retrieve_arg *arg = sbp->template getarg<dhash_retrieve_arg> ();
    ptr<chordID> guess = NULL;
    if (arg->options & DHASHCLIENT_GUESS_SUPPLIED)
      guess = New refcounted<chordID> (arg->guess);
    dhcli->retrieve
      (blockID (arg->blockID, arg->ctype, DHASH_BLOCK),
       wrap (mkref (this), &dhashgateway::retrieve_cb, sbp),
       arg->options, guess);
  }
}

