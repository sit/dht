
/*
 *  Copyright (C) 2002-2003  Massachusetts Institute of Technology
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as
 *  published by the Free Software Foundation; either version 2, or (at
 *  your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 *  USA
 */

#include <dhashgateway_prot.h>
#include "dhash_common.h"
#include "proxy.h"
#include "dbfe.h"
#include "arpc.h"

proxygateway::proxygateway (ptr<axprt_stream> x, ptr<aclnt> l,
                            ptr<dbfe> il, str host, int port)
{
  local = l;
  ilog = il;

  proxyclnt = 0;
  proxyhost = host;
  proxyport = port;
  tcpconnect (proxyhost, proxyport,
              wrap (mkref (this), &proxygateway::proxy_connected, x));
}

void
proxygateway::proxy_connected (ptr<axprt_stream> x, int fd)
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
	                 wrap (mkref (this), &proxygateway::dispatch));
}

proxygateway::~proxygateway ()
{
}

void
proxygateway::dispatch (svccb *sbp)
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

      if (arg->options & DHASHCLIENT_USE_CACHE) {
	ptr<dhash_insert_res> res = New refcounted<dhash_insert_res> ();
	local->call
	  (DHASHPROC_INSERT, arg, res, 
	   wrap (mkref(this), &proxygateway::local_insert_cb, false, sbp, res));
      }

      else if (proxyclnt == 0) {
	arg->options = (arg->options | DHASHCLIENT_USE_CACHE);
	ptr<dhash_insert_res> res = New refcounted<dhash_insert_res> ();
	local->call
	  (DHASHPROC_INSERT, arg, res,
	   wrap (mkref (this), &proxygateway::local_insert_cb, true, sbp, res));
      }

      else {
	int options = arg->options;
	arg->options = (arg->options & (~DHASHCLIENT_USE_CACHE));
	ptr<dhash_insert_res> res = New refcounted<dhash_insert_res> ();
	proxyclnt->call
	  (DHASHPROC_INSERT, arg, res,
	   wrap (mkref (this), &proxygateway::proxy_insert_cb,
	         options, sbp, res));
      }
    }
    break;
    
  case DHASHPROC_RETRIEVE:
    {
      dhash_retrieve_arg *arg = sbp->template getarg<dhash_retrieve_arg> ();
      if ((arg->options & DHASHCLIENT_USE_CACHE) || proxyclnt == 0) {
	arg->options = (arg->options | DHASHCLIENT_USE_CACHE);
	ptr<dhash_retrieve_res> res = New refcounted<dhash_retrieve_res> ();
	local->call
	  (DHASHPROC_RETRIEVE, arg, res,
	   wrap (mkref (this), &proxygateway::local_retrieve_cb, sbp, res));
      }

      else {
	int options = arg->options;
	arg->options =
	  (arg->options & (~DHASHCLIENT_USE_CACHE));
	ptr<dhash_retrieve_res> res = New refcounted<dhash_retrieve_res> ();
	proxyclnt->call
	  (DHASHPROC_RETRIEVE, arg, res,
	   wrap (mkref (this), &proxygateway::proxy_retrieve_cb,
	         options, sbp, res));
      }
    }
    break;
    
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

void
proxygateway::insert_cache_cb (dhash_insert_arg *arg,
                               ptr<dhash_insert_res> res, clnt_stat err)
{
  if (err || res->status)
    warn << "insert into cache failed\n";
  delete arg;
}

void
proxygateway::proxy_insert_cb (int options, svccb *sbp,
                               ptr<dhash_insert_res> res, clnt_stat err)
{
  if (err || res->status) {
    sbp->reply (res);
    return;
  }
  
  dhash_insert_arg *arg = sbp->template getarg<dhash_insert_arg> ();
  arg->options = options;
  
  // this must be before sbp->reply, otherwise sbp object is not
  // guaranteed to be around

  dhash_insert_arg *na = 0;
  if (arg->options & DHASHCLIENT_CACHE) {
    na = New dhash_insert_arg; 
    na->blockID = arg->blockID;
    na->ctype = arg->ctype;
    na->len = arg->len;
    na->block.setsize (na->len);
    memmove (na->block.base (), arg->block.base (), na->len);
    na->options = DHASHCLIENT_USE_CACHE;
  }

  sbp->reply (res);

  if (na) {
    ptr<dhash_insert_res> res = New refcounted<dhash_insert_res> ();
    local->call
      (DHASHPROC_INSERT, na, res,
       wrap (mkref (this), &proxygateway::insert_cache_cb, na, res));
  }
}

void
proxygateway::local_insert_cb (bool disconnected, svccb *sbp,
                               ptr<dhash_insert_res> res, clnt_stat err)
{
  if (disconnected) {
    dhash_insert_arg *arg = sbp->template getarg<dhash_insert_arg> ();
    dhash_ctype t = arg->ctype;
    bigint n = arg->blockID;
    warn << "cannot connect to proxy, remember " << n << "\n";

    ref<dbrec> k = my_id2dbrec (n);
    if (!ilog->lookup (k)) {
      ref<dbrec> d = New refcounted<dbrec> (&t, sizeof (t));
      if (ilog->insert (k, d)) {
        warn << "failed to insert " << n << " into insert log\n";
        dhash_insert_res r (DHASH_RETRY);
        sbp->reply (&r);
        return;
      }
    }
  }

  sbp->reply (res);
}

void
proxygateway::proxy_retrieve_cb (int options, svccb *sbp,
                                 ptr<dhash_retrieve_res> res, clnt_stat err)
{
  if (err || res->status) {
    sbp->reply (res);
    return;
  }

  dhash_retrieve_arg *arg = sbp->template getarg<dhash_retrieve_arg> ();
  arg->options = options;
 
  // this must be before sbp->reply, otherwise sbp object is not
  // guaranteed to be around

  dhash_insert_arg *na = 0;
  if (arg->options & DHASHCLIENT_CACHE) {
    na = New dhash_insert_arg; 
    na->blockID = arg->blockID;
    na->ctype = res->resok->ctype;
    na->len = res->resok->len;
    na->block.setsize (na->len);
    memmove (na->block.base (), res->resok->block.base (), na->len);
    na->options = DHASHCLIENT_USE_CACHE;
    na->guess = 0;
  }

  sbp->reply (res);

  if (na) {
    ptr<dhash_insert_res> res = New refcounted<dhash_insert_res> ();
    local->call
      (DHASHPROC_INSERT, na, res,
       wrap (mkref (this), &proxygateway::insert_cache_cb, na, res));
  }
}

void
proxygateway::local_retrieve_cb (svccb *sbp,
                                 ptr<dhash_retrieve_res> res, clnt_stat err)
{
  if ((!err && res->status == DHASH_OK) || proxyclnt == 0) {
    sbp->reply (res);
    return;
  }

  dhash_retrieve_arg *arg = sbp->template getarg<dhash_retrieve_arg> ();
  int options = arg->options;
  arg->options =
    (arg->options & (~(DHASHCLIENT_USE_CACHE | DHASHCLIENT_CACHE)));
  ptr<dhash_retrieve_res> r = New refcounted<dhash_retrieve_res> ();
  proxyclnt->call
    (DHASHPROC_RETRIEVE, arg, r,
     wrap (mkref (this), &proxygateway::proxy_retrieve_cb, options, sbp, r));
}

