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

dhashclient::dhashclient (ptr<axprt_stream> _x, int n, ptr<chord> node)
  : x (_x), clntnode(node), do_caching (0), nreplica (n)
{
  clntsrv = asrv::alloc (x, dhashclnt_program_1,
			 wrap (this, &dhashclient::dispatch));
}

void
dhashclient::dispatch (svccb *sbp)
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
      
      /* OLD SCHOOL lookup
      clntnode->find_successor (arg->key, 
				wrap (this, &dhashclient::lookup_findsucc_cb, 
				      sbp));
      */
      dhash_fetch_arg *farg = sbp->template getarg<dhash_fetch_arg> ();

      ptr<dhash_fetch_arg> arg = New refcounted<dhash_fetch_arg> (*farg);
      
      chordID next = clntnode->lookup_closestpred (arg->key);
      dhash_fetchiter_res *i_res = New dhash_fetchiter_res (DHASH_CONTINUE);
      
      route path;
      path.push_back (next);
      clntnode->doRPC (next, dhash_program_1, DHASHPROC_FETCHITER, arg, i_res,
		       wrap(this, &dhashclient::lookup_iter_cb, 
			    sbp, i_res, path, 0));

    } 
    break;

  case DHASHPROC_TRANSFER:
    {
      warnt ("DHASH: transfer_request");
      dhash_transfer_arg *targ = sbp->template getarg<dhash_transfer_arg>();
      ptr<dhash_fetch_arg> farg = 
	New refcounted<dhash_fetch_arg>(targ->farg);
      dhash_res *res = New dhash_res (DHASH_OK);
      clntnode->doRPC (targ->source, dhash_program_1, DHASHPROC_FETCH, 
		       farg, res, 
		       wrap (this, &dhashclient::transfer_cb,
			     sbp, res));
    }
    break;
  case DHASHPROC_SEND:
    {
      warnt ("DHASH: send_request");
      dhash_send_arg *sarg = sbp->template getarg<dhash_send_arg>();
      ptr<dhash_insertarg> iarg = 
	New refcounted<dhash_insertarg> (sarg->iarg);
      dhash_storeres *res = New dhash_storeres (DHASH_OK);
      clntnode->doRPC (sarg->dest, dhash_program_1, DHASHPROC_STORE,
		       iarg, res,
		       wrap (this, &dhashclient::send_cb,
			     sbp, res, sarg->dest));
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

    dhash_storeres *res = New dhash_storeres(DHASH_OK);
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
    dhash_storeres *nres = New dhash_storeres(DHASH_OK);
    clntnode->locations->cacheloc (res->pred->p.x, res->pred->p.r);
    clntnode->doRPC(res->pred->p.x, dhash_program_1, 
		    DHASHPROC_STORE, item, nres,
		    wrap(this, &dhashclient::insert_store_cb, sbp, nres, item, source));

  } else
    sbp->reply (res);

  delete res;
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
dhashclient::straddled (route path, chordID &k)
{
  int n = path.size ();
  if (n < 2) return false;
  chordID prev = path[n-1];
  chordID pprev = path[n-2];
  return between (pprev, prev, k);
}

void 
dhashclient::lookup_iter_cb (svccb *sbp, 
			     dhash_fetchiter_res *res,
			     route path,
			     int nerror,
			     clnt_stat err) 
{
  dhash_fetch_arg *arg = sbp->template getarg<dhash_fetch_arg> ();
  ptr<dhash_fetch_arg> rarg = New refcounted<dhash_fetch_arg>(*arg);

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
      /* No more predecessors; lets look for a replica */
      vec<chord_node> succ;
      for (int i = 1; i < NSUCC; i++) {
	chord_node node;
	node.x = clntnode->nth_successorID (i);
	node.r = clntnode->locations->getlocation (node.x)->addr;
	succ.push_back (node);
      }

      query_successors (succ, path.size () + 100*nerror, sbp, rarg, 
			clntnode->clnt_ID ());
    } else {
      dhash_fetchiter_res *nres = New dhash_fetchiter_res (DHASH_CONTINUE);
      /* assumes an in-order RPC transport, otherwise retry
	 might reach prev before alert can update tables*/
      clntnode->doRPC (plast, dhash_program_1, DHASHPROC_FETCHITER, 
		       rarg, nres,
		       wrap(this, &dhashclient::lookup_iter_cb, 
			    sbp, nres, path, nerror));
    }
  } else if (res->status == DHASH_COMPLETE) {
    /* CASE II */
    memorize_block (arg->key, res);
    dhash_res *fres = New dhash_res (DHASH_OK);
    fres->resok->res = res->compl_res->res;
    fres->resok->offset = res->compl_res->offset;
    fres->resok->attr = res->compl_res->attr;
    fres->resok->hops = path.size () + nerror * 100;
    fres->resok->source = path.back ();
    cache_on_path (arg->key, path);
    sbp->reply (fres);
    delete fres;
  } else if (res->status == DHASH_CONTINUE) {
    chordID next = res->cont_res->next.x;
    chordID prev = path.back ();
    //    warn << "node " << prev << " returned " << next << "\n";
    if (next == prev) {
      if (res->cont_res->succ_list.size () == 0) 
	/*CASE III.a */
	sbp->replyref (DHASH_NOENT);
      else {
	/* CASE III.b */
	vec<chord_node> succ;
	for (unsigned int i = 0; i < res->cont_res->succ_list.size (); i++) 
	  succ.push_back (res->cont_res->succ_list[i]);

	query_successors (succ, (int) path.size () + 100 * nerror, sbp, 
			  rarg, prev);
      }
    } else {
      /* CASE IV */
      if (straddled (path, arg->key)) {
	sbp->replyref (DHASH_NOENT);
      } else {
	clntnode->locations->cacheloc (next, res->cont_res->next.r);
	dhash_fetchiter_res *nres = New dhash_fetchiter_res (DHASH_CONTINUE);
	path.push_back (next);
	assert (path.size () < 1000);
	clntnode->doRPC (next, dhash_program_1, DHASHPROC_FETCHITER, 
			 rarg, nres,
			 wrap(this, &dhashclient::lookup_iter_cb, 
			      sbp, nres, path, nerror));
      }
    }
  } else {
    /* the last node queried was responbile but doesn't have it */
    sbp->replyref (DHASH_NOENT);
  }

  delete res;
}

void
dhashclient::query_successors (vec<chord_node> succ, 
			       int pathlen,
			       svccb *sbp,
			       ptr<dhash_fetch_arg> rarg,
			       chordID source)
{
  chord_node first_node = succ.pop_front ();
  clntnode->locations->cacheloc (first_node.x,
				 first_node.r);
  dhash_res *fres = New dhash_res (DHASH_OK);
  query_succ_state *st = New query_succ_state (succ, pathlen, sbp, rarg, source);
  clntnode->doRPC (first_node.x, dhash_program_1, DHASHPROC_FETCH,
		   rarg, fres,
		   wrap (this, &dhashclient::query_successors_fetch_cb,
			 st, first_node.x, fres));
  
}

void
dhashclient::query_successors_fetch_cb (query_succ_state *st,
					chordID prev,
					dhash_res *fres, 
					clnt_stat err) 
{
  if ((err) || (fres->status != DHASH_OK)) {

    if (st->succ.size () == 0) {
      st->sbp->replyref (DHASH_NOENT);
      return;
    }

    if (err) {
      st->pathlen += 100;
      warn << "alerting " << st->source << " about " << prev << " from query_succ\n";
      clntnode->alert (st->source, prev);
    } else 
      st->pathlen++;

    chord_node next_succ = st->succ.pop_front ();
    clntnode->locations->cacheloc (next_succ.x,
				   next_succ.r);
    
    dhash_res *nfres = New dhash_res (DHASH_OK);
    clntnode->doRPC (next_succ.x, dhash_program_1, DHASHPROC_FETCH,
		     st->rarg, nfres,
		     wrap (this, &dhashclient::query_successors_fetch_cb,
			   st, next_succ.x, nfres));
    
  } else if (fres->status == DHASH_OK) {
    fres->resok->hops =  st->pathlen;
    st->sbp->reply (fres);
    delete st;
  } else {
    fatal << "WTF\n";
  }

  delete fres;
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
dhashclient::send_cb (svccb *sbp, dhash_storeres *res, 
		      chordID source, clnt_stat err)
{
  if (err) res->set_status (DHASH_RPCERR);
  else if (res->status == DHASH_RETRY) {
    dhash_send_arg *sarg = sbp->template getarg<dhash_send_arg>();
    ptr<dhash_insertarg> iarg = 
      New refcounted<dhash_insertarg> (sarg->iarg);
    dhash_storeres *nres = New dhash_storeres (DHASH_OK);
    clntnode->locations->cacheloc (nres->pred->p.x, nres->pred->p.r);
    clntnode->doRPC (nres->pred->p.x, dhash_program_1, DHASHPROC_FETCH,
		     iarg, nres, wrap (this, &dhashclient::send_cb,
				       sbp, nres, sarg->dest));
  } else if (res->status == DHASH_OK) {
    sbp->reply (res);
  }
  delete res;
};
				  
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
  if (!do_caching) {
    if (block_memorized (key)) forget_block (key);
    return;
  }
  if (!block_memorized (key)) return;
  for (unsigned int i=0; i < path.size (); i++)
    send_block (key, path[i], DHASH_CACHE);
  forget_block (key);
}


void
dhashclient::send_block (chordID key, chordID to, store_status stat)
{
  store_state *ss = pst[key];
  
  unsigned int mtu = 8192;
  unsigned int off = 0;
  do {
    dhash_storeres *res = New dhash_storeres (DHASH_OK);
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
