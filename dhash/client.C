#include <chord.h>
#include <dhash.h>
#include <chord_prot.h>
#include "sfsmisc.h"
#include "arpc.h"
#include "crypt.h"
#include <sys/time.h>
#include "chord_util.h"

/*
 * Implementation of the distributed hash service.
 */

dhashclient::dhashclient (ptr<axprt_stream> _x)
  : x (_x), do_caching (0), num_replicas(0)
{
  p2pclntsrv = asrv::alloc (x, dhashclnt_program_1,
			 wrap (this, &dhashclient::dispatch));
}

void
dhashclient::dispatch (svccb *sbp)
{
  if (!sbp) {
    delete this;
    return;
  }
  assert (defp2p);

  switch (sbp->proc ()) {
  case DHASHPROC_NULL:
    sbp->reply (NULL);
    return;
  case DHASHPROC_LOOKUP:
    {
      dhash_fetch_arg *arg = sbp->template getarg<dhash_fetch_arg> ();

      warnt("DHASH: lookup_request");

      searchcb_entry *scb = NULL;
#if 0
      if (do_caching)
	scb = defp2p->registerSearchCallback(wrap(this, &dhashclient::search_cb, arg->key));
#endif

      defp2p->dofindsucc (arg->key,wrap(this,&dhashclient::lookup_findsucc_cb, 
				   sbp, scb));
    } 
    break;
  case DHASHPROC_INSERT:
    {
      warnt("DHASH: insert_request");
      dhash_insertarg *item = sbp->template getarg<dhash_insertarg> ();
      ptr<dhash_insertarg> p_item = New refcounted<dhash_insertarg> (*item);
      sfs_ID n = item->key;
      defp2p->insert_or_lookup = true;
      defp2p->dofindsucc (n, wrap(this, &dhashclient::insert_findsucc_cb, 
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
				sfs_ID succ, route path,
				sfsp2pstat err) {
  if (err) {
    dhash_res res;
    warn << "error finding sucessor\n";
    res.set_status(DHASH_CHORDERR);
    sbp->reply(&res);
  } else {

    warnt("DHASH: insert_after_dofindsucc|issue_STORE");

    dhash_storeres *res = New dhash_storeres();
    defp2p->doRPC(succ, dhash_program_1, DHASHPROC_STORE, item, res,
		  wrap(this, &dhashclient::insert_store_cb, sbp, res));

#if 0
    if (do_caching) 
      cache_on_path(item, path);
#endif

  }
}

void
dhashclient::insert_store_cb(svccb *sbp,  dhash_storeres *res, 
			     clnt_stat err )
{

  warnt("DHASH: insert_after_STORE");

  if (res->status) {
    // error in insertion/replication: return to caller
    warnx << "insert_store_cb: failed " << res->status << "\n";
    sbp->replyref(dhash_stat (DHASH_STOREERR));
  } else 
    sbp->replyref(dhash_stat (DHASH_OK));

  delete res;
}
   
void
dhashclient::cache_on_path(ptr<dhash_insertarg> item, route path) 
{

  for (unsigned int i = 0; i < path.size (); i++) {
    warn << "caching " << i << " out of " << path.size () << " on " << path[i] << "\n";
    warn << "item is " << item->key << "\n";
    dhash_stat *res = New dhash_stat();
    defp2p->doRPC(path[i], dhash_program_1, DHASHPROC_STORE, item, res,
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
				searchcb_entry *scb,
				sfs_ID succ, route path,
				sfsp2pstat err)
{
  if (scb)
    defp2p->removeSearchCallback(scb);

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
    retry_state *st = New retry_state (arg->key, sbp, succ, path, scb);
    
    defp2p->doRPC(succ, dhash_program_1, DHASHPROC_FETCH, a, res, 
		  wrap(this, &dhashclient::lookup_fetch_cb, res, st));
  }
}

void
dhashclient::lookup_fetch_cb(dhash_res *res, retry_state *st, clnt_stat err) 
{
  if (err) {
    warnx << "lookup_fetch_cb failed " << err << "\n";
    sfs_ID l = st->path.back ();
    warnx << "lookup_fetch_cb: last " << l << " failed " << st->succ << "\n";
    defp2p->deleteloc (st->succ);
    defp2p->alert (l, st->succ);
    defp2p->dofindsucc (st->n, wrap(this, &dhashclient::lookup_findsucc_cb, 
				   st->sbp, st->scb));
  } else if (res->status == DHASH_RETRY) {
    warnx << "lookup_fetch_cb: retry for " << st->n << " at " 
	  << st->succ << "\n";
    defp2p->get_predecessor (st->succ, wrap (this, &dhashclient::retry, st));
  } else {

    warnt("DHASH: lookup_after_FETCH");


#if 0
    if (do_caching) {
      ptr<dhash_insertarg> di = New refcounted<dhash_insertarg> ();
      di->key = st->n;
      di->data = res->resok->res;
      cache_on_path(di, st->path);
      delete di;
    }
#endif

    st->sbp->reply (res);
    
  }
  delete res;
  delete st;
}

void
dhashclient::retry (retry_state *st, sfs_ID p, net_address a, sfsp2pstat stat)
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
    ptr<sfs_ID> p_n = New refcounted<sfs_ID> (st->n);
    defp2p->doRPC(p, dhash_program_1, DHASHPROC_FETCH, p_n, r, 
		  wrap(this, &dhashclient::lookup_fetch_cb, r,st));
  }
}


// ----------- notification


void
dhashclient::search_cb(sfs_ID my_target, sfs_ID node, sfs_ID target, cbi cb) {


  if (my_target == target) {
    dhash_stat *status = New dhash_stat();
    ptr<sfs_ID> target_h = New refcounted<sfs_ID>();
    *target_h = target;
    defp2p->doRPC (node, dhash_program_1, DHASHPROC_CHECK, target_h, status,
		   wrap(this, &dhashclient::search_cb_cb, status, cb));
  } else
    cb (0);
  
}

void
dhashclient::search_cb_cb (dhash_stat *res, cbi cb, clnt_stat err) {

  if (err) {
    warn << "DHASH_CHECK failed in search_cb\n";
    cb (0);
    return;
  } 
  
  //  warn << "res was " << *res << "\n";
  if (*res != DHASH_NOTPRESENT) {
    warn << "CACHE HIT\n";
    cb (1);
  } else
    cb (0);
}


