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
      //warn << "looking up " << n << " to insert \n";    
      warn << "whatever I got was " << item->data.size () << " bytes long\n";
      
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
  //warn << "insert_findsucc_cb: succ was " << succ << "\n";

  dhash_res *res = New dhash_res();
  if (err) {
    warn << "error finding sucessor\n";
    res->set_status(DHASH_NOENT);
    sbp->reply(res);
  } else {
    //warn << "going to connect to ID " << succ << "\n";
    defp2p->chord_connect(succ, wrap(this, &dhashclient::insert_connect_cb, sbp, item));
  }
}

void
dhashclient::insert_connect_cb(svccb *sbp, dhash_insertarg *item, ptr<axprt_stream> x) 
{
  //  warn << "in insert_connect_cb for " << item->key << "\n";
  dhash_stat *stat = New dhash_stat ();
  if (x == NULL) {
    warn << "connect failed\n";
    sbp->replyref(dhash_stat(DHASH_NOENT));
  } else {
    // XXX - really need to cache these (or use the same streams chord does)
    ptr<aclnt> c = aclnt::alloc (x, dhash_program_1);
    c->call (DHASHPROC_STORE, item, stat, wrap(this, &dhashclient::insert_store_cb, sbp, stat));
  }
}

void
dhashclient::insert_store_cb(svccb *sbp, dhash_stat *res, clnt_stat err) 
{
  warn << "going to return " << *res << " from insert_store_cb\n";
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
    defp2p->chord_connect(succ, wrap(this, &dhashclient::lookup_connect_cb, sbp, n));
  }
}

void
dhashclient::lookup_connect_cb(svccb *sbp, sfs_ID *n, ptr<axprt_stream> x) 
{
  dhash_res *res = New dhash_res();
  if (x == NULL) {
    warn << "connect failed\n";
    res->set_status(DHASH_NOENT);
    sbp->reply(res);
  } else {
    // XXX - really need to cache these (or use the same streams chord does)
    ptr<aclnt> c = aclnt::alloc (x, dhash_program_1);
    c->call (DHASHPROC_FETCH, n, res, wrap(this, &dhashclient::lookup_fetch_cb, sbp, res));
  }
}

void
dhashclient::lookup_fetch_cb(svccb *sbp, dhash_res *res, clnt_stat err) 
{
  //  if (err) 
 //  sbp->reject (DHASH_NOENT);
  //else
    sbp->replyref(*res);
}

// ----------- utility


