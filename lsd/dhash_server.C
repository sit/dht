#include <dhash.h>
#include <dhash_prot.h>
#include <chord.h>
#include <chord_prot.h>
#include <chord_util.h>
#include <dbfe.h>
#include <arpc.h>


dhash::dhash(str dbname, int k, int ss, int cs) :
  key_store(ss), key_cache(cs) {

  db = new dbfe();
  nreplica = k;

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

  key_store.set_flushcb(wrap(this, &dhash::store_flush));
  key_cache.set_flushcb(wrap(this, &dhash::cache_flush));

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
      fetch(*n, wrap(this, &dhash::fetchsvc_cb, sbp, *n));
    }
    break;
  case DHASHPROC_STORE:
    {
      dhash_insertarg *arg = sbp->template getarg<dhash_insertarg> ();
      if (arg->type == DHASH_STORE) {
	sfs_ID p = defp2p->my_pred ();
	sfs_ID m = defp2p->my_ID ();
	if (between (p, m, arg->key)) {
	  store_cbstate *st = 
	    New store_cbstate (sbp, nreplica, arg, wrap (this, 
							 &dhash::storesvc_cb));
	  store(arg->key, arg->data, arg->type, 
		wrap(this, &dhash::storesvc_cb, st));
	  if (st->nreplica > 0)
	    defp2p->get_successor (defp2p->my_ID(), 
				   wrap (this, &dhash::find_replica_cb, st));
	} else {
	  warnx << "dispatch: store retry\n";
	  dhash_storeres *res = New dhash_storeres();
	  res->set_status (DHASH_RETRY);
	  res->pred->n = p;
	}
      } else {
	store_cbstate *st = 
	  New store_cbstate (sbp, 0, NULL, wrap (this, &dhash::storesvc_cb));
	store(arg->key, arg->data, arg->type, 
	      wrap(this, &dhash::storesvc_cb, st));
      }
    }
    break;
  case DHASHPROC_CHECK:
    {
      sfs_ID *n = sbp->template getarg<sfs_ID> ();
      dhash_stat status = key_status (*n);
      sbp->replyref ( status );
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
  
  return;
}

void
dhash::fetchsvc_cb(svccb *sbp, sfs_ID n, ptr<dbrec> val, dhash_stat err) 
{
  dhash_res *res = New dhash_res();
  if (err == DHASH_OK) {
    res->set_status (DHASH_OK);
    res->resok->res.setsize (val->len);
    memcpy (res->resok->res.base (), val->value, val->len);
  } else {
    sfs_ID p = defp2p->my_pred ();
    sfs_ID m = defp2p->my_ID ();
    if (between (p, m, n)) {
      res->set_status(DHASH_NOENT);
    } else {
      warnx << "dispatch: fetch retry\n";
      res->set_status (DHASH_RETRY);
      res->pred->n = p;
    }
  }
  sbp->reply(res);
  delete res;  
}

void
dhash::storesvc_cb(store_cbstate *st, dhash_stat err) {
  warnx << "storesvc_cb: " << st->r << " " << err << "\n";
  st->r--;
  if (st->r <= 0) {
    dhash_storeres *res = New dhash_storeres();
    res->set_status (err);
    st->sbp->reply(res);
  }
}

//---------------- no sbp's below this line --------------

void
dhash::fetch(sfs_ID id, cbvalue cb) 
{
  warn << "FETCHING " << id << "\n";
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
dhash::store(sfs_ID id, dhash_value data, store_status type, cbstore cb)
{
  if (type == DHASH_STORE) warn << "STORING " << id << "\n";
  else if (type == DHASH_CACHE) warn << "CACHING " << id << "\n";
  else if (type == DHASH_REPLICA) warn << "REPLICA " << id << "\n";

  ptr<dbrec> k = id2dbrec(id);
  ptr<dbrec> d = New refcounted<dbrec> (data.base (), data.size ());

  db->insert(k, d, wrap(this, &dhash::store_cb, cb));
  dhash_stat stat;
  if (type == DHASH_STORE) {
    stat = DHASH_STORED;
    key_store.enter (id, &stat);
  } else {
    stat = DHASH_CACHED;
    key_cache.enter (id, &stat);
  }
}

void
dhash::store_cb(cbstore cb, int stat) 
{
  warn << "store stat: " << stat << "\n";
  if (stat != 0) 
    (*cb)(DHASH_NOENT);
  else 
    (*cb)(DHASH_OK);
}

void
dhash::find_replica_cb (store_cbstate *st, sfs_ID s, 
			net_address r, sfsp2pstat status)
{
  if (status) {
    warnx << "find_replica_cb: failure " << status << "\n";
    st->cb (st, DHASH_NOTPRESENT);
  } else {
    warnx << "find_replica_cb: replica " << st->nreplica 
	  << " store at node " << s << "\n";
    st->nreplica--;
    dhash_storeres *res = New dhash_storeres ();
    st->item->type = DHASH_REPLICA;
    defp2p->doRPC(s, dhash_program_1, DHASHPROC_STORE, st->item, res, 
		  wrap(this, &dhash::store_replica_cb, st, res));
    if (st->nreplica > 0) 
      defp2p->get_successor (s, wrap (this, &dhash::find_replica_cb, st));
  }
}

void
dhash::store_replica_cb(store_cbstate *st, dhash_storeres *res, clnt_stat err) 
{
  if (res->status) {
    warnx << "store_replica_cb: error " << res->status << "\n";
  } else {
    warnx << "store_replica_cb: succeeded\n";
  }
  st->cb (st, res->status);
}

// --------- utility

ptr<dbrec>
dhash::id2dbrec(sfs_ID id) 
{
  str whipme = id.getraw ();
  void *key = (void *)whipme.cstr ();
  int len = whipme.len ();
  
  warn << "id2dbrec: " << id << "=" << hexdump(key, len) << "\n";
  ptr<dbrec> q = New refcounted<dbrec> (key, len);
  return q;
}

void
dhash::act_cb(sfs_ID id, char action) {

  //  warn << "node " << id << " just " << action << "ed the network\n";

}

dhash_stat
dhash::key_status(sfs_ID n) {
  const dhash_stat * s_stat = key_store.lookup (n);
  if (s_stat != NULL)
    return *s_stat;
  
  const dhash_stat * c_stat = key_cache.lookup (n);
  if (c_stat != NULL)
    return *c_stat;

  return DHASH_NOTPRESENT;
}

void
dhash::store_flush (sfs_ID key, dhash_stat value) {
  warn << "flushing element " << key << " from store\n";
  ptr<dbrec> k = id2dbrec(key);
  key_cache.enter (key, &value);
  db->del (k, wrap(this, &dhash::store_flush_cb));
}
 
void
dhash::store_flush_cb (int err) {
  if (err) warn << "Error removing element\n";
}

void
dhash::cache_flush (sfs_ID key, dhash_stat value) {
  warn << "flushing element " << key << " from cache\n";
  ptr<dbrec> k = id2dbrec(key);
  db->del (k, wrap(this, &dhash::cache_flush_cb));
}

void
dhash::cache_flush_cb (int err) {
  if (err) warn << "err flushing from cache\n";
}
