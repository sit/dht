#include <dhash.h>
#include <dhash_prot.h>
#include <chord.h>
#include <chord_prot.h>
#include <dbfe.h>
#include <arpc.h>

dhash::dhash(str dbname) {

  db = new dbfe();

  //set up the options we want
  dbOptions opts;
  opts.addOption("opt_async", 1);
  opts.addOption("opt_cachesize", 80000);
  opts.addOption("opt_nodesize", 4096);
  opts.addOption("opt_create", 1);

  warn << dbname << "\n";
  warn << "init dhash\n";
  if (int err = db->opendb(const_cast < char *>(dbname.cstr()), opts)) {
    warn << "open returned: " << strerror(err) << err << "\n";
    exit (-1);
  }

  assert(defp2p);
  //  defp2p->registerActionCallback(wrap(this, &dhash::act_cb));


}

void
dhash::accept(ptr<axprt_stream> x) {
  ptr<asrv> dhashsrv = asrv::alloc (x, dhash_program_1);
  dhashsrv->setcb( wrap (this, &dhash::dispatch, dhashsrv));
}    

void
dhash::dispatch(ptr<asrv> dhashsrv, svccb *sbp) 
{
  if (!sbp) {
    dhashsrv = NULL;
    return;
  }

  switch (sbp->proc ()) {
  case DHASHPROC_FETCH:
    {
      sfs_ID *n = sbp->template getarg<sfs_ID> ();
      fetch(*n, wrap(this, &dhash::fetchsvc_cb, sbp));
    }
    break;
  case DHASHPROC_STORE:
    {
      dhash_insertarg *arg = sbp->template getarg<dhash_insertarg> ();
      store(arg->key, arg->data, arg->type, wrap(this, &dhash::storesvc_cb, sbp));
    }
    break;
  case DHASHPROC_CHECK:
    {
      sfs_ID *n = sbp->template getarg<sfs_ID> ();
      int *status = key_status[*n];
      if (NULL == status) 
	sbp->replyref (dhash_stat (DHASH_NOTPRESENT));
      else
	sbp->replyref (dhash_stat (DHASH_PRESENT));
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
  
  return;
}

void
dhash::fetchsvc_cb(svccb *sbp, ptr<dbrec> val, dhash_stat err) 
{
  
  dhash_res *res = New dhash_res();
  if (err != DHASH_OK) {
    res->set_status(DHASH_NOENT);
  } else {
    res->set_status (DHASH_OK);
    res->resok->res.setsize (val->len);
    memcpy (res->resok->res.base (), val->value, val->len);
  }
   
  sbp->reply(res);
  //  delete res;  
}

void
dhash::storesvc_cb(svccb *sbp, dhash_stat err) {
  sbp->reply(&err);
}

//---------------- no sbp's below this line --------------

void
dhash::fetch(sfs_ID id, cbvalue cb) 
{
  //  warn << "FETCHING " << id << "\n";
  ptr<dbrec> q = id2dbrec(id);
  db->lookup(q, wrap(this, &dhash::fetch_cb, cb));
}

void
dhash::fetch_cb(cbvalue cb, ptr<dbrec> ret) 
{
  if (ret == NULL) {
    (*cb)(NULL, DHASH_NOENT);
    warn << "key not found in DB\n";
  }  else
    (*cb)(ret, DHASH_OK);
}

void 
dhash::store(sfs_ID id, dhash_value data, store_status type, cbstat cb) 
{

#if 0
  if (type == DHASH_STORE) warn << "STORING " << id << "\n";
  else if (type == DHASH_CACHE) warn << "CACHING " << id << "\n";
  else warn << "don't know what the hell I'm doing\n";
#endif

  ptr<dbrec> k = id2dbrec(id);
  ptr<dbrec> d = New refcounted<dbrec> (data.base (), data.size ());

  db->insert(k, d, wrap(this, &dhash::store_cb, cb));
  key_status.insert (id, type);


#if 0
  defp2p->getsuccessor (id, wrap (this, &dhash::find_replica_cb, nreplica, 
				  data, cb));
#endif

}

void
dhash::store_cb(cbstat cb, int stat) {
  if (stat != 0) 
    (*cb)(DHASH_NOENT);
  else 
    (*cb)(DHASH_OK);
}

#if 0
void
dhash::find_replica_cb (int k, dhash_value data, cbstat cb, sfs_ID s)
{
  dhash_stat *stat = New dhash_stat ();
  defp2p->doRPC(succ, dhash_program_1, DHASHPROC_STORE, item, stat, 
		wrap(this, &dhashclient::store_replica_cb, stat));

  k--;
  if (k > 0) {
    defp2p->getsuccessor (id, wrap (this, &dhash::find_replica_cb, k, data, 
				    cb));
  }
}


void
dhash::store_replica_cb(dhash_stat *res, clnt_stat err) 
{
  if (err) {
    sbp->replyref(dhash_stat (DHASH_NOENT));
  }
}
#endif

// --------- utility

ptr<dbrec>
dhash::id2dbrec(sfs_ID id) 
{
  void *key = (void *)id.getraw ().cstr ();
  int len = id.getraw ().len ();
  

  ptr<dbrec> q = New refcounted<dbrec> (key, len);
  return q;
}

void
dhash::act_cb(sfs_ID id, char action) {

  //  warn << "node " << id << " just " << action << "ed the network\n";

}
