#include <dhash.h>
#include <dhash_prot.h>
#include <chord.h>
#include <chord_prot.h>
#include <chord_util.h>
#include <dbfe.h>
#include <arpc.h>

#define REP_DEGREE 5

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

  if (int err = db->opendb(const_cast < char *>(dbname.cstr()), opts)) {
    warn << "open returned: " << strerror(err) << err << "\n";
    exit (-1);
  }
  
  assert(defp2p);
  defp2p->registerActionCallback(wrap(this, &dhash::act_cb));

  key_store.set_flushcb(wrap(this, &dhash::store_flush));
  key_cache.set_flushcb(wrap(this, &dhash::cache_flush));

  pred = defp2p->my_pred ();
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

      warnt("DHASH: FETCH_request");

      sfs_ID *n = sbp->template getarg<sfs_ID> ();
      fetch(*n, wrap(this, &dhash::fetchsvc_cb, sbp, *n));
    }
    break;
  case DHASHPROC_STORE:
    {

      warnt("DHASH: STORE_request");

      dhash_insertarg *arg = sbp->template getarg<dhash_insertarg> ();
      if (arg->type == DHASH_STORE) {
	if (responsible (arg->key)) {
	  warnt("DHASH: will store");
	  store(arg->key, arg->data, arg->type, 
		wrap(this, &dhash::storesvc_cb, sbp));
	} else {
	  warnt("DHASH: retry");
	  dhash_storeres *res = New dhash_storeres();
	  res->set_status (DHASH_RETRY);
	  res->pred->n = defp2p->my_pred ();
	} 
      } else {
	warnt("DHASH: will store");
	store(arg->key, arg->data, arg->type, 
	      wrap(this, &dhash::storesvc_cb, sbp));
      }
    }
    break;
  case DHASHPROC_CHECK:
    {

      warnt("DHASH: CHECK_request");
	    
      sfs_ID *n = sbp->template getarg<sfs_ID> ();
      dhash_stat status = key_status (*n);
      sbp->replyref ( status );
      
      warnt("DHASH: CHECK_done");
      
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
  pred = defp2p->my_pred ();
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

  warnt("DHASH: FETCH_replying");
  sbp->reply(res);
  delete res;  
}

void
dhash::storesvc_cb(svccb *sbp, dhash_stat err) {
  
  warnt("DHASH: STORE_replying");

  dhash_storeres *res = New dhash_storeres();
  res->set_status (err);
  sbp->reply(res); 
}


// ----------   chord triggered callbacks -------- 
void
dhash::act_cb(sfs_ID id, char action) {

  warn << "node " << id << " just " << action << "ed the network\n";
  sfs_ID m = defp2p->my_ID ();

  if (action == ACT_NODE_LEAVE) {
    //lost a replica?
    if (replicas.size () > 0) {
      sfs_ID final_replica = replicas.back ();
      if (between (m, final_replica, id)) 
	key_store.traverse (wrap (this, &dhash::rereplicate_cb));
    }

    //lost a master? (handle in fetch)
  } else if (action == ACT_NODE_JOIN) {
    //new node should store some of this node's keys?
    //    warn << "Is " << id << " between " << pred << " and " << m << "\n";
    if (between (pred, m, id)) {
      // warn << "YES\n";
      key_store.traverse (wrap (this, &dhash::walk_cb, pred, id));
    }
#if 0
    //new node should be a replica?
    if (replicas.size () > 0) {
      sfs_ID final_replica = replicas.back ();
      if (between (m, final_replica, id)) 
	key_store.traverse (wrap (this, &dhash::fix_replicas_cb, id));
    }
#endif

    pred = defp2p->my_pred ();
  } else {
    fatal << "update action not supported\n";
  }

}

void
dhash::walk_cb(sfs_ID pred, sfs_ID id, sfs_ID k) 
{
  if ((key_status (k) == DHASH_STORED) && (between (pred, id, k))) {
    warn << k << " is between " << pred << " and " << id << "\n";
    transfer_key (id, k, DHASH_STORE, wrap(this, &dhash::transfer_key_cb, k));
  }
}

void
dhash::transfer_key_cb (sfs_ID key, dhash_stat err)
{

  if (err == DHASH_OK) {
    dhash_stat status = key_status (key);
    key_store.remove (key);
    key_cache.enter (key, &status);
  }
}

void
dhash::fix_replicas_cb (sfs_ID id, sfs_ID k) 
{
  if (key_status (k) == DHASH_REPLICATED) 
    transfer_key (id, k, DHASH_REPLICA, wrap (this, &dhash::fix_replicas_transfer_cb));
  
}

void
dhash::fix_replicas_transfer_cb (dhash_stat err) 
{
  if (err) warn << "error transferring replicated key\n";
}

void
dhash::rereplicate_cb (sfs_ID k) 
{
  if (key_status (k) == DHASH_STORED)
    replicate_key (k, REP_DEGREE, wrap (this, &dhash::rereplicate_replicate_cb));
}

void
dhash::rereplicate_replicate_cb (dhash_stat err) 
{
  if (err) warn << "replication error\n";
}
//---------------- no sbp's below this line --------------

void
dhash::transfer_key (sfs_ID to, sfs_ID key, store_status stat, callback<void, dhash_stat>::ref cb) 
{
  fetch(key, wrap(this, &dhash::transfer_fetch_cb, to, key, stat, cb));
}

void
dhash::transfer_fetch_cb (sfs_ID to, sfs_ID key, store_status stat, callback<void, dhash_stat>::ref cb,
			  ptr<dbrec> data, dhash_stat err) 
{
  
  if (err) {
    cb (err);
  } else {
    dhash_storeres *res = New dhash_storeres ();
    ptr<dhash_insertarg> i_arg = New refcounted<dhash_insertarg> ();
    i_arg->key = key;
    i_arg->data.setsize (data->len);
    i_arg->type = stat;
    memcpy(i_arg->data.base (), data->value, data->len);

    if (stat == DHASH_STORE)
      warn << "going to transfer (STORE) " << key << " to " << to << "\n";
    else 
      warn << "going to transfer (REPLICA) " << key << " to " << to << "\n";

    defp2p->doRPC(to, dhash_program_1, DHASHPROC_STORE, i_arg, res,
		  wrap(this, &dhash::transfer_store_cb, cb, res));
  }
  
}

void
dhash::transfer_store_cb (callback<void, dhash_stat>::ref cb, 
			  dhash_storeres *res, clnt_stat err) 
{
  if (err) 
    cb (DHASH_RPCERR);
  else 
    cb (res->status);

  delete res;
}

void
dhash::fetch(sfs_ID id, cbvalue cb) 
{
  warnt("DHASH: FETCH_before_db");

  ptr<dbrec> q = id2dbrec(id);
  db->lookup(q, wrap(this, &dhash::fetch_cb, cb));

  if (key_status (id) == DHASH_REPLICATED) {
    //a request for a replicated key implies the owner
    // has departed. upgrade to onwer status
    key_store.remove (id);
    dhash_stat status = DHASH_STORED;
    key_store.enter (id, &status);
  }
}

void
dhash::fetch_cb (cbvalue cb, ptr<dbrec> ret) 
{

  warnt("DHASH: FETCH_after_db");

  if (ret == NULL) {
    (*cb)(NULL, DHASH_NOENT);
    warn << "key not found in DB\n";
  } else
    (*cb)(ret, DHASH_OK);
}

void 
dhash::store (sfs_ID id, dhash_value data, store_status type, cbstore cb)
{

  ptr<dbrec> k = id2dbrec(id);
  ptr<dbrec> d = New refcounted<dbrec> (data.base (), data.size ());

  warnt("DHASH: STORE_before_db");

  db->insert (k, d, wrap(this, &dhash::store_cb, type, id, cb));
  dhash_stat stat;
  if (type == DHASH_STORE) {
    stat = DHASH_STORED;
    key_store.enter (id, &stat);
  } else if (type == DHASH_REPLICA) {
    stat = DHASH_REPLICATED;
    key_store.enter (id, &stat);
  } else {
    stat = DHASH_CACHED;
    key_cache.enter (id, &stat);
  }
}

void
dhash::store_cb(store_status type, sfs_ID id, cbstore cb, int stat) 
{
  warnt("DHASH: STORE_after_db");

  if (stat != 0) 
    (*cb)(DHASH_STOREERR);
  else if (type == DHASH_STORE)
    replicate_key (id, REP_DEGREE, wrap (this, &dhash::store_repl_cb, cb));
  else	   
    (*cb)(DHASH_OK);
}

void
dhash::store_repl_cb (cbstore cb, dhash_stat err) 
{
  if (err) 
    cb (err);
  else
    cb (DHASH_OK);
}

void 
dhash::replicate_key (sfs_ID key, int degree, callback<void, dhash_stat>::ref cb) 
{
  warnt ("DHASH: replicate_key");
  if (degree > 0) {
    sfs_ID succ = defp2p->my_succ ();
    vec<sfs_ID> repls;
    transfer_key (succ, key, DHASH_REPLICA, wrap (this, &dhash::replicate_key_transfer_cb,
						  key, degree, cb, succ, repls));
  } else
    cb (DHASH_OK);
}

void
dhash::replicate_key_succ_cb (sfs_ID key, int degree_remaining, callback<void, dhash_stat>::ref cb,
			      vec<sfs_ID> repls,
			      sfs_ID succ, sfsp2pstat err) 
{
  warnt ("DHASH: replicate_key_succ_cb");
  if (err) {
    cb (DHASH_CHORDERR);
  } else {
    repls.push_back (succ);
    transfer_key (succ, key, DHASH_REPLICA, wrap (this, &dhash::replicate_key_transfer_cb,
						  key, degree_remaining, cb, succ, repls));
  }
}

void
dhash::replicate_key_transfer_cb (sfs_ID key, int degree_remaining, callback<void, dhash_stat>::ref cb,
				  sfs_ID succ, vec<sfs_ID> repls,
				  dhash_stat err)
{

  warnt ("DHASH: replicate_key_transfer_cb");
  if (err) {
    cb (err);
  } else if (degree_remaining == 1) {
    //update the list of replica servers
    replicas = repls;
    cb (DHASH_OK);
  } else {
    defp2p->get_succ (succ, wrap (this, &dhash::replicate_key_succ_cb, 
				  key, 
				  degree_remaining - 1,
				  cb, repls));
  }
}

// --------- utility

ptr<dbrec>
dhash::id2dbrec(sfs_ID id) 
{
  str whipme = id.getraw ();
  void *key = (void *)whipme.cstr ();
  int len = whipme.len ();
  
  //  warn << "id2dbrec: " << id << "=" << hexdump(key, len) << "\n";
  ptr<dbrec> q = New refcounted<dbrec> (key, len);
  return q;
}

dhash_stat
dhash::key_status(sfs_ID n) {
  dhash_stat * s_stat = key_store.peek (n);
  if (s_stat != NULL)
    return *s_stat;
  
  dhash_stat * c_stat = key_cache.peek (n);
  if (c_stat != NULL)
    return *c_stat;

  return DHASH_NOTPRESENT;
}

char
dhash::responsible(sfs_ID n) 
{
    sfs_ID p = defp2p->my_pred ();
    sfs_ID m = defp2p->my_ID ();
    return (between (p, m, n));
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


