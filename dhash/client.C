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

dhashclient::dhashclient (ptr<axprt_stream> _x, ptr<chord> node)
  : x (_x), clntnode(node), do_caching (0)
{
  clntsrv = asrv::alloc (x, dhashclnt_program_1,
			 wrap (this, &dhashclient::dispatch));
}

void
dhashclient::dispatch (svccb *sbp)
{
  if (!sbp) {
    delete this;
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
      
      /* OLD SCHOOL lookup
      clntnode->find_successor (arg->key, 
				wrap (this, &dhashclient::lookup_findsucc_cb, 
				      sbp));
      */
      dhash_fetch_arg *farg = sbp->template getarg<dhash_fetch_arg>();

      ptr<dhash_fetch_arg> arg = New refcounted<dhash_fetch_arg> (*farg);

      chordID next = clntnode->lookup_closestpred (arg->key);
      dhash_fetchiter_res *i_res = New dhash_fetchiter_res ();
      
      route path;
      path.push_back (next);
      clntnode->doRPC (next, dhash_program_1, DHASHPROC_FETCHITER, arg, i_res,
		       wrap(this, &dhashclient::lookup_iter_cb, 
			    sbp, i_res, next, path));

    } 
    break;

  case DHASHPROC_TRANSFER:
    {
      warnt ("DHASH: transfer_request");
      dhash_transfer_arg *targ = sbp->template getarg<dhash_transfer_arg>();
      ptr<dhash_fetch_arg> farg = 
	New refcounted<dhash_fetch_arg>(targ->farg);
      dhash_res *res = New dhash_res ();
      clntnode->doRPC (targ->source, dhash_program_1, DHASHPROC_FETCH, 
		       farg, res, 
		       wrap (this, &dhashclient::transfer_cb,
			     sbp, res));
    }
    break;
  case DHASHPROC_INSERT:
    {
      warnt("DHASH: insert_request");
      dhash_insertarg *item = sbp->template getarg<dhash_insertarg> ();
      memorize_block (item);
      ptr<dhash_insertarg> p_item = New refcounted<dhash_insertarg> (*item);
      chordID n = item->key;
      clntnode->find_successor (n, wrap(this, &dhashclient::insert_findsucc_cb,
				    sbp, p_item));
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

void
dhashclient::insert_findsucc_cb(svccb *sbp, ptr<dhash_insertarg> item,
				chordID succ, route path,
				chordstat err) {
  if (err) {
    dhash_res res;
    warn << "error finding sucessor\n";
    res.set_status(DHASH_CHORDERR);
    sbp->reply(&res);
  } else {

    warnt("DHASH: insert_after_dofindsucc|issue_STORE");

    dhash_storeres *res = New dhash_storeres();
    clntnode->doRPC(succ, dhash_program_1, DHASHPROC_STORE, item, res,
		wrap(this, &dhashclient::insert_store_cb, sbp, res, item, succ));
    
    cache_on_path (item->key, path);
  }
}

void
dhashclient::insert_store_cb(svccb *sbp, dhash_storeres *res, 
			     ptr<dhash_insertarg> item,
			     chordID source,
			     clnt_stat err )
{
  warnt("DHASH: insert_after_STORE");
  if (res->status == DHASH_RETRY) {
    dhash_storeres *nres = New dhash_storeres();
    clntnode->locations->cacheloc (res->pred->p.x, res->pred->p.r, source);
    clntnode->doRPC(res->pred->p.x, dhash_program_1, 
		    DHASHPROC_STORE, item, nres,
		    wrap(this, &dhashclient::insert_store_cb, sbp, nres, item, source));

  } else
    sbp->reply (res);

  delete res;
}

void 
dhashclient::lookup_iter_cb (svccb *sbp, 
			     dhash_fetchiter_res *res,
			     chordID prev,
			     route path,
			     clnt_stat err) 
{
  dhash_fetch_arg *arg = sbp->template getarg<dhash_fetch_arg> ();
  if (err) {
    sbp->replyref (DHASH_RPCERR);
  } else if (res->status == DHASH_COMPLETE) {
    memorize_block (arg->key, res);
    dhash_res *fres = New dhash_res (DHASH_OK);
    fres->resok->res = res->compl_res->res;
    fres->resok->offset = res->compl_res->offset;
    fres->resok->attr = res->compl_res->attr;
    fres->resok->hops = path.size ();
    fres->resok->source = prev;
    sbp->reply (fres);
    cache_on_path (arg->key, path);
  } else if (res->status == DHASH_CONTINUE) {
    chordID next = res->cont_res->next.x;
    clntnode->locations->cacheloc (next, res->cont_res->next.r, prev);
    dhash_fetchiter_res *nres = New dhash_fetchiter_res;
    path.push_back (next);
    ptr<dhash_fetch_arg> rarg = New refcounted<dhash_fetch_arg>(*arg);
    clntnode->doRPC (next, dhash_program_1, DHASHPROC_FETCHITER, 
		     rarg, nres,
		     wrap(this, &dhashclient::lookup_iter_cb, 
			  sbp, nres, next, path));
  } else 
    sbp->replyref (DHASH_NOENT);

  delete res;
}

void
dhashclient::transfer_cb (svccb *sbp, dhash_res *res, clnt_stat err)
{
  if (err) res->set_status (DHASH_RPCERR);
  else if (res->status == DHASH_OK) res->resok->hops = 0;
  
  sbp->reply (res);
  delete res;
}				  
void
dhashclient::memorize_block (dhash_insertarg *item) 
{
  memorize_block (item->key, item->attr.size, item->offset, 
		  item->data.base(), item->data.size ());
}
void
dhashclient::memorize_block (chordID key, dhash_fetchiter_res *res) 
{
  if (res->status != DHASH_COMPLETE) return;
  memorize_block (key, res->compl_res->attr.size,
		  res->compl_res->offset,
		  res->compl_res->res.base (),
		  res->compl_res->res.size ());
}
void
dhashclient::memorize_block (chordID key, int tsize, 
			     int offset, void *base, int dsize)
{
  store_state *ss = pst[key];
  if (!ss) {
    store_state nss (tsize);
    pst.insert(key, nss);
    ss = pst[key];
  } 
  ss->read += dsize;
  memcpy (ss->buf + offset, base, dsize);
}

bool
dhashclient::block_memorized (chordID key) 
{
  store_state *ss = pst[key];
  return (ss->read == ss->size);
}

void 
dhashclient::forget_block (chordID key)
{
  store_state *ss = pst[key];
  delete ss->buf;
  pst.remove (key);
}

void
dhashclient::cache_on_path (chordID key, route path)
{
  if (!block_memorized (key)) return;
  for (unsigned int i=0; i < path.size (); i++)
    send_block (key, path[i], DHASH_CACHE);
}

void
dhashclient::send_block (chordID key, chordID to, store_status stat)
{
  store_state *ss = pst[key];
  
  unsigned int mtu = 8192;
  unsigned int off = 0;
  do {
    dhash_storeres *res = New dhash_storeres ();
    ptr<dhash_insertarg> i_arg = New refcounted<dhash_insertarg> ();
    i_arg->key = key;
    i_arg->offset = off;
    int remain = (off + mtu <= static_cast<unsigned long>(ss->size)) ? 
      mtu : ss->size - off;
    i_arg->data.setsize (remain);
    i_arg->attr.size = ss->size;
    i_arg->type = stat;
    memcpy(i_arg->data.base (), ss->buf + off, remain);
    
    clntnode->doRPC(to, dhash_program_1, DHASHPROC_STORE, 
		    i_arg, res,
		    wrap(this, &dhashclient::send_store_cb, res));
    off += remain;
  } while (off < static_cast<unsigned long>(ss->size));
}
  
void
dhashclient::send_store_cb (dhash_storeres *res, clnt_stat err) 
{
  if ((err) || (res->status))  warn << "Error caching block\n";
  delete res;
}
