
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

#include <sys/types.h>
#include "sfsmisc.h"
#include "arpc.h"
#include "async.h"
#include "str.h"
#include "parseopt.h"
#include "refcnt.h"
#include "bigint.h"
#include "dbfe.h"
#include "dhashgateway_prot.h"
#include "dhash_common.h"
#include "merkle_misc.h"
#include "proxy.h"

extern ptr<aclnt> local;
extern ptr<dbfe> ilog;
extern ptr<dbfe> cache_db;
extern str proxyhost;
extern int proxyport;

void proxy_sync ();
static void sync_next (bigint n, ptr<aclnt> proxyclnt);

static int
sync_dbnext (ptr<dbfe> db, bigint &next)
{
  ptr<dbEnumeration> enumer = db->enumerate ();
  ptr<dbPair> d = enumer->nextElement (my_id2dbrec (next)); // >=
  if (!d)
    d = enumer->firstElement ();
  if (d) {
    next = my_dbrec2id (d->key);
    return 0;
  }
  else
    return -1; // db is empty
}

static void
sync_insert (bigint n, ptr<aclnt> proxyclnt,
             ptr<dhash_insert_res> res, clnt_stat err)
{
  if (err) {
    warn << "RPC to proxy failed, try again later\n";
    delaycb (60, wrap (proxy_sync));
    return;
  }

  if (!res->status) {
    ref<dbrec> k = my_id2dbrec (n);
    ilog->del (k);
    warn << n << " moved to proxy, removed\n";
  }
  else
    warn << n << " insert to proxy failed (" << res->status << ")\n";
  delaycb (0, wrap (sync_next, n+1, proxyclnt));
}

static void
sync_retrieve (bigint n, ptr<aclnt> proxyclnt,
               dhash_retrieve_res* res)
{
  if (!res->status) {
    dhash_insert_arg a;
    a.blockID = n;
    a.ctype = res->resok->ctype;
    a.len = res->resok->len;
    a.block.setsize (a.len);
    memmove (a.block.base (), res->resok->block.base (), a.len);
    a.options = DHASHCLIENT_SUCCLIST_OPT;
    a.guess = 0;

    ptr<dhash_insert_res> r = New refcounted<dhash_insert_res> ();
    proxyclnt->call
      (DHASHPROC_INSERT, &a, r, wrap (sync_insert, n, proxyclnt, r));
  }
  else {
    warn << "cannot retrieve " << n << " from local cache\n";
    delaycb (0, wrap (sync_next, n+1, proxyclnt));
  }
}

static void
sync_next (bigint n, ptr<aclnt> proxyclnt)
{
  if (sync_dbnext (ilog, n)) {
    delaycb (60, wrap (proxy_sync));
    return;
  }

  ref<dbrec> k = my_id2dbrec (n);
  ref<dbrec> d = ilog->lookup (k);
  if (!d) {
    warn << "can't retrieve " << n << " from insert log\n";
    delaycb (0, wrap (sync_next, n+1, proxyclnt));
    return;
  }

  dhash_ctype t;
  assert (d->len == sizeof (t));
  memmove (&t, d->value, d->len);

  dhash_retrieve_arg a;
  a.blockID = n;
  a.ctype = t;
  a.options = DHASHCLIENT_USE_CACHE;

  warn << "syncing " << n << "to remote dhash\n";
  ptr<dbrec> cache_ret = cache_db->lookup (id2dbrec(a.blockID));
  assert(cache_ret);

  ptr<dhash_retrieve_res> res = block_to_res(cache_ret, t);
  sync_retrieve(n, proxyclnt, res);
}

static void
sync_connected (int fd)
{
  if (fd < 0) {
    delaycb (60, wrap (proxy_sync));
    return;
  }
  ref<axprt_stream> x = axprt_stream::alloc (fd, 1024*1025);
  ptr<aclnt> proxyclnt = aclnt::alloc (x, dhashgateway_program_1);
  sync_next (0, proxyclnt);
}

void
proxy_sync ()
{
  tcpconnect (proxyhost, proxyport, wrap (sync_connected));
}

