#include <chord.h>
#include <dhash.h>
#include <chord_prot.h>

/*
 * Implementation of the distributed hash service.
 */

dhashclient::dhashclient (ptr<axprt_stream> _x)
  : x (_x)
{
  p2pclntsrv = asrv::alloc (x, dhashclnt_program_1,
			 wrap (this, &dhashclient::dispatch));

  defp2p->registerActionCallback(wrap(this, &dhashclient::act_cb));
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
      sfs_ID *n = sbp->template getarg<sfs_ID> ();
      defp2p->dofindsucc (*n, wrap(this, &dhashclient::lookup_findsucc_cb, 
				   sbp, n));
    } 
    break;
  case DHASHPROC_INSERT:
    {
      dhash_insertarg *item = sbp->template getarg<dhash_insertarg> ();
      sfs_ID n = item->key;
      
      defp2p->dofindsucc (n, wrap(this, &dhashclient::insert_findsucc_cb, 
				  sbp, item));
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

void
dhashclient::insert_findsucc_cb(svccb *sbp, dhash_insertarg *item,
				sfs_ID succ, route path,
				sfsp2pstat err) {

  dhash_res *res = New dhash_res();
  if (err) {
    warn << "error finding sucessor\n";
    res->set_status(DHASH_NOENT);
    sbp->reply(res);
  } else {
    dhash_stat *stat = New dhash_stat ();
    defp2p->doRPC(succ, dhash_program_1, DHASHPROC_STORE, item, stat, 
		  wrap(this, &dhashclient::insert_store_cb, sbp, stat));
  }
}

void
dhashclient::insert_store_cb(svccb *sbp, dhash_stat *res, clnt_stat err) 
{
  if (err) {
    sbp->replyref(dhash_stat (DHASH_NOENT));
  } else
    sbp->reply(res);
}

void
dhashclient::lookup_findsucc_cb(svccb *sbp, sfs_ID *n,
				sfs_ID succ, route path,
				sfsp2pstat err) 
{
  dhash_res *res = New dhash_res();
  if (err) {
    res->set_status(DHASH_NOENT);
    sbp->reply(res);
  } else {
    dhash_res *res = New dhash_res();
    defp2p->doRPC(succ, dhash_program_1, DHASHPROC_FETCH, n, res, 
		  wrap(this, &dhashclient::lookup_fetch_cb, sbp, res));
  }
}

void
dhashclient::lookup_fetch_cb(svccb *sbp, dhash_res *res, clnt_stat err) 
{
  if (err) 
    sbp->reject (SYSTEM_ERR);
  else
    sbp->replyref(*res);
}

// ----------- notification

void
dhashclient::act_cb(sfs_ID id, char action) {

  warn << "node " << id << " just " << action << "ed the network\n";

}


