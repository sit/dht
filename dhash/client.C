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
/*
 * Implementation of the distributed hash service.
 */

int n = 0;

dhashgateway::dhashgateway (ptr<axprt_stream> _x, ptr<chord> node)
  : x (_x), clntnode(node), do_caching (false)
{
  clntsrv = asrv::alloc (x, dhashgateway_program_1,
			 wrap (this, &dhashgateway::dispatch));
}

void
dhashgateway::dispatch (svccb *sbp)
{
  if (!sbp) {
    return;
  }
  assert (clntnode);

  switch (sbp->proc ()) {
  case DHASHPROC_NULL:
    sbp->reply (NULL);
    return;
  case DHASHPROC_LOOKUP:
    {
      warnt("DHASH: lookup_request");
      
      dhash_fetch_arg *farg = sbp->template getarg<dhash_fetch_arg> ();
      
      ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg> ();
      arg->key = farg->key;
      arg->start = farg->start;
      arg->len = farg->len;
      arg->v.n = clntnode->clnt_ID ();

      dhash_fetchiter_res *i_res = New dhash_fetchiter_res (DHASH_CONTINUE);
      route path;
      path.push_back (clntnode->clnt_ID ());
      doRPC (path[0], dhash_program_1, DHASHPROC_FETCHITER, arg, i_res,
		       wrap(this, &dhashgateway::lookup_iter_cb, 
			    sbp, i_res, path, 0));
    } 
    break;

  case DHASHPROC_TRANSFER:
    {
      warnt ("DHASH: transfer_request");
      dhash_transfer_arg *targ = sbp->template getarg<dhash_transfer_arg>();
      ptr<s_dhash_fetch_arg> farg = New refcounted<s_dhash_fetch_arg> ();
      farg->key = targ->farg.key;
      farg->start = targ->farg.start;
      farg->len = targ->farg.len;
      farg->v.n = targ->source;

      dhash_fetchiter_res *res = New dhash_fetchiter_res (DHASH_OK);
      doRPC (targ->source, dhash_program_1, DHASHPROC_FETCHITER, 
		       farg, res, 
		       wrap (this, &dhashgateway::transfer_cb,
			     farg->key, sbp, res));
    }
    break;
  case DHASHPROC_SEND:
    {
      warnt ("DHASH: send_request");
      dhash_send_arg *sarg = sbp->template getarg<dhash_send_arg>();
      ptr<s_dhash_insertarg> iarg = 
	New refcounted<s_dhash_insertarg> ();
      iarg->v.n = sarg->dest;
      iarg->key = sarg->iarg.key;
      iarg->data = sarg->iarg.data;
      iarg->offset = sarg->iarg.offset;
      iarg->type = sarg->iarg.type;
      iarg->attr = sarg->iarg.attr;

      dhash_storeres *res = New dhash_storeres (DHASH_OK);
      doRPC (sarg->dest, dhash_program_1, DHASHPROC_STORE,
		       iarg, res,
		       wrap (this, &dhashgateway::send_cb,
			     sbp, res, iarg));
    }
    break;
  case DHASHPROC_INSERT:
    {
      warnt("DHASH: insert_request");
      dhash_insertarg *item = sbp->template getarg<dhash_insertarg> ();
      ptr<dhash_insertarg> p_item = New refcounted<dhash_insertarg> (*item);
      chordID n = item->key;
      clntnode->find_successor (n, 
				wrap(this, &dhashgateway::insert_findsucc_cb,
				     sbp, p_item));
    }
    break;
  case DHASHPROC_ACTIVE:
    {
      warnt("DHASH: change_active_request");
      int32 *n =  sbp->template getarg<int32> ();
      clntnode->set_active (*n);
      sbp->replyref (DHASH_OK);
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

void
dhashgateway::insert_findsucc_cb(svccb *sbp, ptr<dhash_insertarg> item,
				chordID succ, route path,
				chordstat err) {
  if (err) {
    dhash_res res;
    warn << "error finding sucessor\n";
    res.set_status(DHASH_CHORDERR);
    *res.hops = path.size()-1;
    sbp->reply(&res);
  } else {
    
    ptr<s_dhash_insertarg> s_item = 
      New refcounted<s_dhash_insertarg> ();
    s_item->v.n = succ;
    s_item->key = item->key;
    s_item->data = item->data;
    s_item->offset = item->offset;
    s_item->type = item->type;
    s_item->attr = item->attr;

    warnt("DHASH: insert_after_dofindsucc|issue_STORE");

    dhash_storeres *res = New dhash_storeres(DHASH_OK);
    doRPC(succ, dhash_program_1, DHASHPROC_STORE, s_item, res,
	  wrap(this, &dhashgateway::insert_store_cb, sbp, res, s_item, succ, 1));
    
  }
}

void
dhashgateway::insert_store_cb(svccb *sbp, dhash_storeres *res, 
			     ptr<s_dhash_insertarg> item,
			     chordID source,
			     unsigned int attempts,
			     clnt_stat err )
{
#define DHASH_MAX_INSERT_ATTEMPTS 10

  warnt("DHASH: insert_after_STORE");
  if (res->status == DHASH_RETRY) {
    if (attempts < DHASH_MAX_INSERT_ATTEMPTS) {
      attempts++;
      dhash_storeres *nres = New dhash_storeres(DHASH_OK);
      clntnode->locations->cacheloc (res->pred->p.x, res->pred->p.r);
      doRPC(res->pred->p.x, dhash_program_1, 
	    DHASHPROC_STORE, item, nres,
	    wrap(this, &dhashgateway::insert_store_cb, sbp, nres, item, source, attempts));
    } else {
      warn << "store failed, attempts " << DHASH_MAX_INSERT_ATTEMPTS << "\n";
      dhash_storeres res2 (DHASH_ERR); 
      sbp->reply (&res2);
    }
  } else {
    sbp->reply (res);
    delete res;
  }
}


/* This callback is responsible for finding data in the distributed store. 
   The basic operation of the lookup procedure is as follows:
   - query local node for prediction of key K's successor, call prediction N
   - query N for either a) the data associated with K or b) a new prediction
   - recurse on new prediction


   A case-by-case analysis of its behavior follows:

   CASE I: An RPC error has occurred; the node that we were directed to is 
   down, unbeknownst to the node that routed us there. Alert previous node to 
   the failure and retry the iteration

   CASE II: The remote node has returned the data associated with key either
   because it is responsible for storing the data or because it has cached
   or replicated the data. Return the data to the caller.

   CASE III: Case three encompasses situations in which the remote node does
   not store the requested data and has instead returned a new prediction
   CASE III.b: The remote node is unable to return a better prediction than 
   its own ID. In this case we query, one by one, the R successors that the 
   node also returns. The hope being that one of these successors replicates
   the data; this is likely since replicas are stored on the R successors
   of a node.
   CASE III.a: The remote node is not able to return a better guess and
   is also not able to return a better prediction. Return failure.

   CASE IV: The remote node's prediction is different than its ID. Recurse.
*/

bool
dhashgateway::straddled (route path, chordID &k)
{
  int n = path.size ();
  if (n < 2) return false;
  chordID prev = path[n-1];
  chordID pprev = path[n-2];
  return between (pprev, prev, k);
}

static void
replyerror(svccb *sbp, dhash_stat stat, int hops)
{
  assert (stat != DHASH_OK);
  dhash_res *res = New dhash_res (stat);
  *res->hops = hops;
  sbp->reply (res);
  delete res;
}
  

void 
dhashgateway::lookup_iter_cb (svccb *sbp, 
			     dhash_fetchiter_res *res,
			     route path,
			     int nerror,
			     clnt_stat err) 
{
  dhash_fetch_arg *arg = sbp->template getarg<dhash_fetch_arg> ();
  ptr<s_dhash_fetch_arg> rarg = New refcounted<s_dhash_fetch_arg>();
  rarg->key = arg->key;
  rarg->start = arg->start;
  rarg->len = arg->len;

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
      plast = clntnode->lookup_closestpred (rarg->key);
      path.push_back (plast);
    }
    if (plast == clntnode->clnt_ID ()) {
      replyerror (sbp, DHASH_NOENT, path.size()-1 + nerror*100);
    } else {
      dhash_fetchiter_res *nres = New dhash_fetchiter_res (DHASH_CONTINUE);
      /* assumes an in-order RPC transport, otherwise retry
	 might reach prev before alert can update tables*/
      rarg->v.n = plast;
      doRPC (plast, dhash_program_1, DHASHPROC_FETCHITER, 
		       rarg, nres,
		       wrap(this, &dhashgateway::lookup_iter_cb, 
			    sbp, nres, path, nerror));
    }
  } else if (res->status == DHASH_COMPLETE) {
    /* CASE II */
    // a zero size just checks if the block exists, ie. no download occurs
    if (res->compl_res->res.size() > 0) {
      save_chunk (arg->key, res, path);
      cache_on_path (arg->key, path);
    }
    dhash_res fres (DHASH_OK);
    fres.resok->res = res->compl_res->res;
    fres.resok->offset = res->compl_res->offset;
    fres.resok->attr = res->compl_res->attr;
    fres.resok->hops = path.size()-1 + nerror*100;
    fres.resok->source = path.back ();
    sbp->reply (&fres);
  } else if (res->status == DHASH_CONTINUE) {
    chordID next = res->cont_res->next.x;
    chordID prev = path.back ();
    if ((next == prev) || (straddled (path, arg->key))) {
      replyerror (sbp, DHASH_NOENT, path.size()-1 + nerror*100);
    } else {
      clntnode->locations->cacheloc (next, res->cont_res->next.r);
      dhash_fetchiter_res *nres = New dhash_fetchiter_res (DHASH_CONTINUE);
      path.push_back (next);
      assert (path.size () < 1000);

      rarg->v.n = next;
      doRPC (next, dhash_program_1, DHASHPROC_FETCHITER, 
	     rarg, nres,
	     wrap(this, &dhashgateway::lookup_iter_cb, 
		  sbp, nres, path, nerror));
    }

  } else {
    /* the last node queried was responbile but doesn't have it */
    replyerror (sbp, DHASH_NOENT, path.size()-1 + nerror*100);
  }

  delete res;
}

void
iterres2res (dhash_fetchiter_res *ires, dhash_res *res) 
{
    res->resok->offset = ires->compl_res->offset;
    res->resok->attr = ires->compl_res->attr;
    res->resok->source = ires->compl_res->source;
    res->resok->res = ires->compl_res->res;
    res->resok->hops = 0;
}

void
dhashgateway::transfer_cb (chordID key, svccb *sbp, 
			  dhash_fetchiter_res *ires, clnt_stat err)
{
  dhash_res *res = New dhash_res(DHASH_OK);
  if ((err) || (ires->status != DHASH_COMPLETE)) 
    res->set_status (DHASH_RPCERR);
  else {
    iterres2res(ires, res);
    save_chunk (key, ires);
    assert (pst[key]);
    cache_on_path (key, pst[key]->path);
  }
  sbp->reply (res);
  delete res;
  delete ires;
}

void
dhashgateway::send_cb (svccb *sbp, dhash_storeres *res, 
		      ptr<s_dhash_insertarg> iarg, clnt_stat err)
{
  if (err) res->set_status (DHASH_RPCERR);
  else if (res->status == DHASH_RETRY) {
    dhash_storeres *nres = New dhash_storeres (DHASH_OK);
    clntnode->locations->cacheloc (nres->pred->p.x, nres->pred->p.r);
    doRPC (nres->pred->p.x, dhash_program_1, DHASHPROC_STORE,
		     iarg, nres, wrap (this, &dhashgateway::send_cb,
				       sbp, nres, iarg));
  } else if (res->status == DHASH_OK) {
    sbp->reply (res);
  }
  delete res;
};
				  
void
dhashgateway::save_chunk (chordID key, dhash_fetchiter_res *res, route path) 
{
  if (res->status != DHASH_COMPLETE)
    return;
  save_chunk (key, res->compl_res->attr.size,
		  res->compl_res->offset,
		  res->compl_res->res.base (),
		  res->compl_res->res.size ());
  store_state *ss = pst[key];
  assert (ss);
  ss->path = path;

}
void
dhashgateway::save_chunk (chordID key, dhash_fetchiter_res *res) 
{
  if (res->status != DHASH_COMPLETE)
    return;
  save_chunk (key, res->compl_res->attr.size,
		  res->compl_res->offset,
		  res->compl_res->res.base (),
		  res->compl_res->res.size ());
}

static void
join(store_chunk *c)
{
  store_chunk *cnext;

  while (c->next && c->end >= c->next->start) {
    cnext = c->next;
    if (c->end < cnext->end)
      c->end = cnext->end;
    c->next = cnext->next;
    delete cnext;
  }
}

bool
store_state::iscomplete()
{
  return have && have->start==0 && have->end==(unsigned)size;
}

bool
store_state::addchunk(unsigned int start, unsigned int end, void *base)
{
  store_chunk **l, *c;

#if 0
  warn << "store_state";
  for(c=have; c; c=c->next)
    warnx << " [" << (int)c->start << " " << (int)c->end << "]";
  warnx << "; add [" << (int)start << " " << (int)end << "]\n";
#endif

  if(start >= end)
    return false;

  if(start >= end || end > size)
    return false;
  
  l = &have;
  for (l=&have; *l; l=&(*l)->next) {
    c = *l;
    // our start touches this block
    if (c->start <= start && start <= c->end) {
      // we have new data
      if (end > c->end) {
        memmove (buf+start, base, end-start);
        c->end = end;
        join(c);
      }
      return true;
    }
    // our start comes before this block; break to insert
    if (start < c->start)
      break;
  }
  *l = New store_chunk(start, end, *l);
  memmove(buf+start, base, end-start);
  join(*l);
  return true;
}


void
dhashgateway::save_chunk (chordID key, int tsize, 
			     int start, void *base, int dsize)
{
  store_state *ss = pst[key];
  if (!ss) {
    store_state *nss = New store_state(key, tsize);
    pst.insert(nss);
    ss = nss;
  } 
  ss->addchunk(start, start+dsize, base);
}

bool
dhashgateway::block_complete (chordID key) 
{
  store_state *ss = pst[key];
  return ss && ss->iscomplete();
}

void 
dhashgateway::forget_block (chordID key)
{
  store_state *ss = pst[key];
  if (ss) {
    pst.remove (ss);
    delete ss;
  }
}

void
dhashgateway::cache_on_path (chordID key, route path)
{

#if 0
  for (unsigned int i=0; i<path.size (); i++)
    warnx << " " << path[i];
  warnx << "\n";
#endif

  if (!block_complete (key)) 
    return;

  if (do_caching && path.size () >= 2)
    send_block (key, path[path.size () - 2], DHASH_CACHE);

  forget_block (key);
}


void
dhashgateway::send_block (chordID key, chordID to, store_status stat)
{
  store_state *ss = pst[key];
  
  unsigned int mtu = 8192;
  unsigned int off = 0;
  do {
    dhash_storeres *res = New dhash_storeres (DHASH_OK);
    ptr<s_dhash_insertarg> i_arg = New refcounted<s_dhash_insertarg> ();
    i_arg->v.n = to;
    i_arg->key = key;
    i_arg->offset = off;
    int remain = (off + mtu <= static_cast<unsigned long>(ss->size)) ? 
      mtu : ss->size - off;
    i_arg->data.setsize (remain);
    i_arg->attr.size = ss->size;
    i_arg->type = stat;
    memcpy(i_arg->data.base (), ss->buf + off, remain);
    
    doRPC(to, dhash_program_1, DHASHPROC_STORE, 
		    i_arg, res,
		    wrap(this, &dhashgateway::send_store_cb, res));
    off += remain;
  } while (off < static_cast<unsigned long>(ss->size));
}
  
void
dhashgateway::send_store_cb (dhash_storeres *res, clnt_stat err) 
{
  if ((err) || (res->status))  warn << "Error caching block\n";
  delete res;
}

/* ------- wrappers ------ */
void
dhashgateway::doRPC (chordID ID, rpc_program prog, int procno,
		    ptr<void> in, void *out, aclnt_cb cb) 
{
  clntnode->doRPC (ID, prog, procno,
		   in, out, cb);
}


// ------------------------------------------------------------------------
// ------------------------------------------------------------------------


// XXX make the MTU an argument
#define FOO_MTU 1024

// XXX this really belongs in some util file.
bigint compute_hash (const void *buf, size_t buflen)
{
  char h[sha1::hashsize];
  bzero(h, sha1::hashsize);
  sha1_hash (h, buf, buflen);
  
  bigint n;
  mpz_set_rawmag_be(&n, h, sha1::hashsize);  // For big endian
  return n;
}



class dhash_insert {
protected:
  ptr<aclnt> gwclnt;
  uint npending_rpcs;
  bool error;
  bigint key;
  dhash_block block;
  cbinsert_t cb;
  dhash_ctype auth_type;

  dhash_insert (ptr<aclnt> gwclnt, bigint key, const char *buf, size_t buflen,
		cbinsert_t cb, dhash_ctype t)
    : gwclnt (gwclnt), npending_rpcs (0), error (false), key (key),
      block (buf, buflen), cb (cb), auth_type (t)
  {
    if (auth_type == DHASH_CONTENTHASH)
      check_exists ();  // elide the insert if that hash already exists
    else
      step1 ();
  }

  void check_exists ()
  {
    ptr<dhash_res> res = New refcounted<dhash_res> (DHASH_OK);
    dhash_fetch_arg arg;
    arg.key = key;
    arg.len = 0;
    arg.start = 0;
    gwclnt->call (DHASHPROC_LOOKUP, &arg, res,
		 wrap (this, &dhash_insert::check_exists_cb, res));
  }

  void check_exists_cb (ptr<dhash_res> res, clnt_stat err)
  {
    if (!err && res->status == DHASH_OK) {
      ///warn << "exists: " << key << "\n";
      // if the data is already inserted, report back success
      (*cb) (false, key);
      delete this;
    } else {
      ///warn << "NOT exists: " << key << "\n";
      // otherwise, we need to insert the data
      step1 ();
    }
  }


  void step1 ()
  {
    uint nwrite = MIN (FOO_MTU, block.len); 

    ptr<dhash_storeres> res = New refcounted<dhash_storeres>();

    dhash_insertarg arg;
    arg.type = DHASH_STORE;
    arg.attr.size = block.len;
    arg.attr.ctype = auth_type;
    arg.key = key;
    arg.offset = 0;
    arg.data.setsize (nwrite);
    memcpy(arg.data.base (), block.data, nwrite);

    
    npending_rpcs++;
    gwclnt->call (DHASHPROC_INSERT, &arg, res,
		 wrap(this, &dhash_insert::step2, nwrite, res));

  }

  void step2 (uint nwritten, ptr<dhash_storeres> res, clnt_stat err)
  {
    if (!err && res->status == DHASH_OK) {
      dhash_send_arg arg;
      arg.dest = res->resok->source;
      arg.iarg.type = DHASH_STORE;
      arg.iarg.attr.size = block.len;
      arg.iarg.attr.ctype = auth_type;
      arg.iarg.key = this->key;
      
      while (nwritten < block.len) {
	unsigned int sz = MIN (FOO_MTU, block.len - nwritten);
	ptr<dhash_storeres> nres = New refcounted<dhash_storeres>();

	arg.iarg.offset = nwritten;
	arg.iarg.data.setsize (sz);
	memcpy (arg.iarg.data.base (), &block.data[nwritten], sz);
	
	npending_rpcs++;
	gwclnt->call (DHASHPROC_SEND, &arg, nres,
		     wrap (this, &dhash_insert::finish, nres));
	nwritten += sz;
      }
    }

    finish (res, err);
  }
    

  void finish (ptr<dhash_storeres> res, clnt_stat err)
  {
    npending_rpcs--;

    if (err)
      fail (strbuf () << "rpc error " << err);
    else if (res->status != DHASH_OK) 
      fail (dhasherr2str (res->status));

    if (npending_rpcs == 0) {
      ///warn << "finished insert: " << key << "\n";
      (*cb) (error, key);
      delete this;
    }
  }

  void fail (str errstr)
  {
    if (errstr)
      warn << "dhash_insert failed: " << key << ": " << errstr << "\n";

    error = true;
  }

public:

  static void execute (ptr<aclnt> gwclnt, bigint key, const char *buf,
		       size_t buflen, cbinsert_t cb, dhash_ctype t)
  {
    vNew dhash_insert (gwclnt, key, buf, buflen, cb, t);
  }
};




class dhash_retrieve {
protected:
  ptr<aclnt> gwclnt;
  uint npending_rpcs;
  bool error;
  bigint key;
  cbretrieve_t cb;  
  ptr<dhash_block> block;
  bool verify;
  dhash_ctype t;

  dhash_retrieve (ptr<aclnt> gwclnt, bigint key, dhash_ctype type,
		  cbretrieve_t cb, bool verify)
    : gwclnt (gwclnt), npending_rpcs (0), error (false), key (key), cb (cb),
      block (NULL), verify (verify), t (type)
  {
    step1();
  }

  void step1 ()
  {
    ptr<dhash_res> res = New refcounted<dhash_res> (DHASH_OK);
    dhash_fetch_arg arg;
    arg.key = key;
    arg.len = FOO_MTU;
    arg.start = 0;
    npending_rpcs++;
    gwclnt->call (DHASHPROC_LOOKUP, &arg, res,
		 wrap (this, &dhash_retrieve::step2, res));
  }


  void step2 (ptr<dhash_res> res, clnt_stat err)
  {

    if (!err && res->status == DHASH_OK) {
      size_t nread = res->resok->res.size ();
      dhash_transfer_arg arg;
      arg.source = res->resok->source;
      arg.farg.key = key;

      // XXX get rid of the cast..
      block = New refcounted<dhash_block> ((char *)NULL, 
					   res->resok->attr.size);

      while (nread < res->resok->attr.size) {
	ptr<dhash_res> nres = New refcounted<dhash_res> (DHASH_OK);
	arg.farg.start = nread;
	arg.farg.len = MIN (FOO_MTU, res->resok->attr.size - nread);
	npending_rpcs++;
	gwclnt->call (DHASHPROC_TRANSFER, &arg, nres,
		     wrap (this, &dhash_retrieve::finish, nres));
	nread += arg.farg.len;
      }
    }

    finish (res, err);
  }
    

  void finish (ptr<dhash_res> res, clnt_stat err)
  {
    npending_rpcs--;

    if (err) 
      fail (strbuf () << "rpc error " << err);
    else if (res->status != DHASH_OK) 
      fail (dhasherr2str (res->status));
    else {
      uint32 off = res->resok->offset;
      uint32 len = res->resok->res.size ();
      if (off + len > block->len) 
	fail (strbuf() << "bad fragment: off " << off << ", len " << len
	      << ", block " << block->len);
      else
	memcpy (&block->data[off], res->resok->res.base (), len);
    }

    if (npending_rpcs == 0) {
      if (error)
	(*cb) (NULL);
      else if (verify && 
	       !dhash::verify (key, t, block->data, block->len)) {
	fail (strbuf() << "data did not verify " << key);
	(*cb) (NULL);
      } else {
	ptr<dhash_block> contents = get_block_contents (block, t);
	(*cb) (contents);
      }
      delete this;
    }
  }

  void fail (str errstr)
  {
    warn << "dhash_retrieve failed: " << key << ": " << errstr << "\n";
    error = true;
  }

  ptr<dhash_block> 
  get_block_contents (ptr<dhash_block> block, dhash_ctype t) 
  {
    switch (t) {
    case DHASH_CONTENTHASH:
    case DHASH_NOAUTH:
      return block;
      break;
    case DHASH_KEYHASH:
      {
	bigint a,b;

	long contentlen;
	xdrmem x1 (block->data, (unsigned)block->len, XDR_DECODE);
	if (!xdr_getbigint (&x1, a) || 
	    !xdr_getbigint (&x1, b) ||
	    !XDR_GETLONG (&x1, &contentlen))
	return NULL;
	
	char *content;
	if (!(content = (char *)XDR_INLINE (&x1, contentlen)))
	  return NULL;
	
	ptr<dhash_block> ret = New refcounted<dhash_block>
	  (content, contentlen);
	return ret;
      }
      break;
    default:
      return NULL;
    }
  }

public:

  static void execute (ptr<aclnt> gwclnt, bigint key, dhash_ctype t,
		       cbretrieve_t cb, bool verify)
  {
    vNew dhash_retrieve (gwclnt, key, t, cb, verify);
  }
};


dhashclient::dhashclient(str sockname)
{
  int fd = unixsocket_connect(sockname);
  if (fd < 0) {
    fatal ("dhashclient: Error connecting to %s: %s\n",
	   sockname.cstr (), strerror (errno));
  }

  gwclnt = aclnt::alloc (axprt_unix::alloc (fd), dhashgateway_program_1);
}

void
dhashclient::insert (const char *buf, size_t buflen, cbinsert_t cb)
{
  bigint key = compute_hash (buf, buflen);
  insert (key, buf, buflen, cb, DHASH_CONTENTHASH);
}


/* 
 * Public Key convention:
 * 
 * bigint pub_key
 * bigint sig
 * long datalen
 * char block_data[datalen]
 *
 */
void
dhashclient::insert (const char *buf, size_t buflen, 
		     rabin_priv key, cbinsert_t cb)
{
  bigint pubkey = key.n;
  str pk_raw = pubkey.getraw ();
  chordID pkID = compute_hash (pk_raw.cstr (), pk_raw.len ());

  str msg (buf, buflen);
  bigint sig = key.sign (msg);

  xdrsuio x;
  int size = buflen + 3 & ~3;
  char *m_buf;
  if (xdr_putbigint (&x, pubkey) &&
      xdr_putbigint (&x, sig) &&
      XDR_PUTLONG (&x, (long int *)&buflen) &&
      (m_buf = (char *)XDR_INLINE (&x, size)))
    {
      memcpy (m_buf, buf, buflen);
      
      int m_len = x.uio ()->resid ();
      char *m_dat = suio_flatten (x.uio ());
      insert (pkID, m_dat, m_len, cb, DHASH_KEYHASH);
      xfree (m_dat);
    } else {
      cb (true, pkID); // marshalling failed.
    }
}


void
dhashclient::insert (bigint key, const char *buf, 
		     size_t buflen, cbinsert_t cb,
		     dhash_ctype t)
{
  dhash_insert::execute (gwclnt, key, buf, buflen, cb, t);
}

void
dhashclient::retrieve (bigint key, dhash_ctype type, cbretrieve_t cb)
{
  dhash_retrieve::execute (gwclnt, key, type, cb, true);
}

bool
dhashclient::sync_setactive (int32 n)
{
  dhash_stat res;
  clnt_stat err = gwclnt->scall (DHASHPROC_ACTIVE, &n, &res);

  if (err)
    warn << "sync_setactive: rpc error " << err << "\n";
  else if (res != DHASH_OK)
    warn << "sync_setactive: dhash error " << dhasherr2str (res) << "\n";
  
  return (err || res != DHASH_OK);
}

