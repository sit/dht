
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
#include "verify.h"
#include "merkle_misc.h"

ptr<dhash_retrieve_res>
block_to_res (ptr<dbrec> val, dhash_ctype ctype)
{
  ptr<dhash_retrieve_res> res = New refcounted<dhash_retrieve_res> (DHASH_OK);
  int n = val->len;

  // YIPAL: not sure if I have to set these to anything.
  res->resok->ctype = ctype;
  res->resok->len = n;
  res->resok->hops = 0;
  res->resok->errors = 0;
  res->resok->retries = 0;
  res->resok->path.setsize(0);
  res->resok->block.setsize(n);
  memcpy (res->resok->block.base (), val->value, n);

  return res;
}


bool
is_keyhash_stale (ref<dbrec> prev, ref<dbrec> d)
{
  long v0 = keyhash_version (prev);
  long v1 = keyhash_version (d);
  if (v0 >= v1)
    return true;
  return false;
}


proxygateway::proxygateway (ptr<axprt_stream> x, ptr<dbfe> cache,
                            ptr<dbfe> dl, str host, int port)
{
  cache_db = cache;
  disconnect_log = dl;

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
proxygateway::insert_to_localcache(chordID id, char* block, int32_t len, dhash_ctype ctype) {
  // insert into DB
  ref<dbrec> k = id2dbrec(id);
  ref<dbrec> d = New refcounted<dbrec> (block, len);
  ptr<dbrec> prev;

  if (!verify (id, ctype, block, len)) {
    warn  << "proxy: cannot verify (" << len << ") " << id << " bytes\n";
    assert(0);
  }

  switch(ctype) {
  case DHASH_CONTENTHASH:
    if (!cache_db->lookup (k)) {
      cache_db->insert (k, d);
      warn << "db write: " << ctype << " " << id << " " << len << "\n";
    } else {
      warn << "db write: " << ctype << " " << id << " already in block cache.\n";
    }
  case DHASH_KEYHASH: 
    prev = cache_db->lookup(k);
    if (prev) {
      if (is_keyhash_stale(prev, d)) {
	break;
      }
      else {
	warn << "db write: " << ctype << " " << id << " with " << len << " bytes (replacing block).\n";
	cache_db->del(k);
      }
    }
    cache_db->insert(k, d);
    warn << "db write: " << ctype << " " << id << " " << len << "\n";
    break;

  case DHASH_NOAUTH:
  case DHASH_APPEND:
  case DHASH_UNKNOWN:
  default:
    warn << "proxy can't handle inserting ctype: " << ctype << "\n";
  }

  return;
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
	insert_to_localcache(arg->blockID, arg->block.base(), arg->block.size(), arg->ctype);
	local_insert_done(false, sbp);
      }

      else if (proxyclnt == 0) {
	arg->options = (arg->options | DHASHCLIENT_USE_CACHE);
	insert_to_localcache(arg->blockID, arg->block.base(), arg->block.size(), arg->ctype);
	local_insert_done(true, sbp);
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
      ptr<dbrec> cache_ret;

      if (arg->options & DHASHCLIENT_USE_CACHE) {
	cache_ret = cache_db->lookup (id2dbrec (arg->blockID));
      }

      if (cache_ret) {
	warn << "using cached block " << cache_ret->len << " " << arg->blockID << "\n";
	ptr<dhash_retrieve_res> res = block_to_res (cache_ret, arg->ctype);
	sbp->reply (res);
	return;
          
      } else if (proxyclnt) {
	int options = arg->options;
	arg->options =
	  (arg->options & (~DHASHCLIENT_USE_CACHE));
	ptr<dhash_retrieve_res> res = New refcounted<dhash_retrieve_res> ();
	proxyclnt->call
	  (DHASHPROC_RETRIEVE, arg, res,
	   wrap (mkref (this), &proxygateway::proxy_retrieve_cb,
	         options, sbp, res));

      }  else {
	dhash_retrieve_res res (DHASH_NOENT);
	sbp->reply (&res);
      }

    }
    break;
    
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

void
proxygateway::proxy_insert_cb (int options, svccb *sbp,
                               ptr<dhash_insert_res> res, clnt_stat err)
{
  if (err || res->status) {
    if (err)
      res->set_status (DHASH_RPCERR);
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
    insert_to_localcache(na->blockID, na->block.base(), na->len, na->ctype);
  }
}

void
proxygateway::local_insert_done (bool disconnected, svccb *sbp)
{
  if (disconnected) {
    dhash_insert_arg *arg = sbp->template getarg<dhash_insert_arg> ();
    dhash_ctype t = arg->ctype;
    bigint n = arg->blockID;
    warn << "cannot connect to proxy, remember " << n << "\n";

    ref<dbrec> k = my_id2dbrec (n);
    if (!disconnect_log->lookup (k)) {
      ref<dbrec> d = New refcounted<dbrec> (&t, sizeof (t));
      if (disconnect_log->insert (k, d)) {
        warn << "failed to insert " << n << " into insert log\n";
        dhash_insert_res r (DHASH_RETRY);
        sbp->reply (&r);
        return;
      }
    }
  }

  ptr<dhash_insert_res> res = New refcounted<dhash_insert_res> (DHASH_OK);
  sbp->reply (res);
}

void
proxygateway::proxy_retrieve_cb (int options, svccb *sbp,
                                 ptr<dhash_retrieve_res> res, clnt_stat err)
{
  if (err || res->status) {
    if (err)
      res->set_status (DHASH_RPCERR);
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
    insert_to_localcache(na->blockID, na->block.base(), na->len, na->ctype);
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


