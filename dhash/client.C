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
  : x (_x), clntnode(node), do_caching (0), num_replicas(0)
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
      dhash_fetch_arg *arg = sbp->template getarg<dhash_fetch_arg> ();
      
      warnt("DHASH: lookup_request");
            clntnode->find_successor (arg->key, 
				wrap (this, &dhashclient::lookup_findsucc_cb, 
				     sbp));
    } 
    break;
  case DHASHPROC_INSERT:
    {
      warnt("DHASH: insert_request");
      dhash_insertarg *item = sbp->template getarg<dhash_insertarg> ();
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
		wrap(this, &dhashclient::insert_store_cb, sbp, res));
    
  }
}

void
dhashclient::insert_store_cb(svccb *sbp,  dhash_storeres *res, 
			     clnt_stat err )
{

  warnt("DHASH: insert_after_STORE");
#if 0 
  if (res->status) {
    // error in insertion/replication: return to caller
    warnx << "insert_store_cb: failed " << res->status << "\n";
    sbp->replyref(dhash_stat (DHASH_STOREERR));
  } else 
    sbp->replyref(dhash_stat (DHASH_OK));
#endif
  sbp->reply (res);
  delete res;
}
   
void
dhashclient::cache_on_path(ptr<dhash_insertarg> item, route path) 
{

  for (unsigned int i = 0; i < path.size (); i++) {
    warn << "caching " << i << " out of " << path.size () << " on " << path[i] << "\n";
    warn << "item is " << item->key << "\n";
    dhash_stat *res = New dhash_stat();
    clntnode->doRPC(path[i], dhash_program_1, DHASHPROC_STORE, item, res,
		wrap(this, &dhashclient::cache_store_cb, res));
  }
}

void
dhashclient::cache_store_cb(dhash_stat *res, clnt_stat err) 
{

  if (err) {
    warnt("DHASH: cache failed");
  } else {
    warnt("DHASH: propogated item");
  }
  delete res;

}

void
dhashclient::lookup_findsucc_cb(svccb *sbp, 
				chordID succ, route path,
				chordstat err)
{

  if (err) {
    warnx << "lookup_findsucc_cb: FETCH FAILURE " << err << "\n";
    dhash_res res;
    res.set_status (DHASH_CHORDERR);
    sbp->reply (&res);
  } else {

    warnt("DHASH: lookup_after_dofindsucc");

    dhash_fetch_arg *arg = sbp->template getarg<dhash_fetch_arg>();
    ptr<dhash_fetch_arg> a = New refcounted<dhash_fetch_arg> (*arg);
    dhash_res *res = New dhash_res(DHASH_OK);
    retry_state *st = New retry_state (arg->key, sbp, succ, path);
    st->hops = path.size ();
    clntnode->doRPC(succ, dhash_program_1, DHASHPROC_FETCH, a, res, 
		wrap(this, &dhashclient::lookup_fetch_cb, res, st));
  }
}

void
dhashclient::lookup_fetch_cb(dhash_res *res, retry_state *st, clnt_stat err) 
{
  if (err) {
    warnx << "lookup_fetch_cb failed " << err << "\n";
    chordID l = st->path.back ();
    warnx << "lookup_fetch_cb: last " << l << " failed " << st->succ << "\n";
#if 0
    defp2p->deleteloc (st->succ);
    defp2p->alert (l, st->succ);
#endif
    clntnode->find_successor (st->n, 
			      wrap(this, &dhashclient::lookup_findsucc_cb, 
				   st->sbp));
  } else if (res->status == DHASH_RETRY) {
    warnx << "lookup_fetch_cb: retry for " << st->n << " at " 
	  << st->succ << "\n";
    clntnode->get_predecessor (st->succ, wrap (this, &dhashclient::retry, st));
  } else if (res->status == DHASH_OK) {

    warnt("DHASH: lookup_after_FETCH");
    res->resok->hops = st->hops;
    st->sbp->reply (res);
    
  } else {
    warn << "error on lookup " << res->status << "\n";
    st->sbp->reply (res);
  }
  delete res;
  delete st;
}

void
dhashclient::retry (retry_state *st, chordID p, net_address a, chordstat stat)
{
  if (stat) {
    warnx << "retry: failure FETCH FAILURE " << st->n << " err " 
	  << stat << "\n";
    dhash_res *res = New dhash_res();
    res->set_status (DHASH_NOENT);
    st->sbp->reply (res);
  } else {
    dhash_res *r = New dhash_res();
    warnx << "retry: " << st->n << " at " << p << "\n";
    ptr<chordID> p_n = New refcounted<chordID> (st->n);
    clntnode->doRPC(p, dhash_program_1, DHASHPROC_FETCH, p_n, r, 
		wrap(this, &dhashclient::lookup_fetch_cb, r,st));
  }
}

