#include <dhash.h>
#include <dhash_prot.h>
#include <chord.h>
#include <chord_prot.h>
#include <chord_util.h>
#include <dbfe.h>
#include <arpc.h>
#ifdef DMALLOC
#include <dmalloc.h>
#endif

#define REP_DEGREE 0

dhash::dhash(str dbname, ptr<vnode> node, int k, int ss, int cs) :
  host_node (node), key_store(ss), key_cache(cs) {

  db = New dbfe();
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
  
  assert(host_node);
  //  defp2p->registerActionCallback(wrap(this, &dhash::act_cb));

  key_store.set_flushcb(wrap(this, &dhash::store_flush));
  key_cache.set_flushcb(wrap(this, &dhash::cache_flush));

  pred = host_node->my_pred ();

  // RPC demux
  host_node->addHandler (DHASH_PROGRAM, wrap(this, &dhash::dispatch));
}

void
dhash::dispatch(unsigned long procno, 
		chord_RPC_arg *arg,
		unsigned long rpc_id) 
{

  char *marshalled_arg = arg->marshalled_args.base ();
  int arg_len = arg->marshalled_args.size ();

  xdrmem x (marshalled_arg, arg_len, XDR_DECODE);
  xdrproc_t proc = dhash_program_1.tbl[procno].xdr_arg;

  switch (procno) {
  case DHASHPROC_FETCH:
    {

      dhash_fetch_arg *farg = New dhash_fetch_arg ();
      if (!proc (x.xdrp (), farg)) {
	warn << "DHASH: error unmarshalling arguments\n";
	return;
      }
      warnt ("DHASH: FETCH_request");
      fetch (farg->key, wrap (this, &dhash::fetchsvc_cb, rpc_id, farg));
    }
    break;

  case DHASHPROC_STORE:
    {

      warnt("DHASH: STORE_request");

      dhash_insertarg *sarg = New dhash_insertarg ();
      if (!proc (x.xdrp (), sarg)) {
	warn << "DHASH: error unmarshalling arguments\n";
	return;
      }
     
      if ((sarg->type == DHASH_STORE) && (!responsible (sarg->key))) {
	warnt("DHASH: retry");
	dhash_storeres *res = New dhash_storeres();
	res->set_status (DHASH_RETRY);
	res->pred->n = host_node->my_pred ();
      } else {
	warnt("DHASH: will store");
	store(sarg, wrap(this, &dhash::storesvc_cb, rpc_id, sarg));	
      }
      
    }
    break;
#if 0
  case DHASHPROC_CHECK:
    {

      warnt("DHASH: CHECK_request");
	    
      chordID *n = sbp->template getarg<chordID> ();
      dhash_stat status = key_status (*n);
      sbp->replyref ( status );
      
      warnt("DHASH: CHECK_done");
      
    }
    break;
#endif
  default:
    break;
  }
  pred = host_node->my_pred ();
  //  delete arg;
  return;
}

void
dhash::fetchsvc_cb (long xid,
		    dhash_fetch_arg *arg, 
		    ptr<dbrec> val, 
		    dhash_stat err)
{
  dhash_res *res = New dhash_res ();

  if (err == DHASH_OK) {
    res->set_status (DHASH_OK);
    int n;
    if (arg->len == 0) {
      n = val->len;
      arg->start = 0;
    } else
      n = (arg->len + arg->start < val->len) ? arg->len : val->len - arg->start;
    
    res->resok->res.setsize (n);
    res->resok->attr.size = val->len;
    res->resok->offset = arg->start;
    memcpy (res->resok->res.base (), (char *)val->value + arg->start, n);
  } else if (responsible (arg->key)) {
    res->set_status (DHASH_NOENT);
  } else {
    warnx << "partial_fetch: retry\n";
    res->set_status (DHASH_RETRY);
    res->pred->n = host_node->my_pred ();
  }

  warnt("DHASH: FETCH_replying");

  dhash_reply (xid, DHASHPROC_FETCH, res);
  delete res;  
  delete arg;
}
void
dhash::storesvc_cb(long xid,
		   dhash_insertarg *arg,
		   dhash_stat err) {
  
  warnt("DHASH: STORE_replying");

  dhash_storeres *res = New dhash_storeres();
  if (err == DHASH_STORE_PARTIAL) {
    res->set_status (DHASH_OK);
    res->resok->done = false;
  } else if (err == DHASH_OK) {
    res->set_status (DHASH_OK);
    res->resok->done = true;
  } else
    res->set_status (err);

  dhash_reply (xid, DHASHPROC_STORE, res);
  delete res;
  delete arg;
}


// ----------   chord triggered callbacks -------- 
void
dhash::act_cb(chordID id, char action) {

  warn << "node " << id << " just " << action << "ed the network\n";
  chordID m = host_node->my_ID ();

  if (action == ACT_NODE_LEAVE) {
#if 0 
   //lost a replica?
    if (replicas.size () > 0) {
      chordID final_replica = replicas.back ();
      if (between (m, final_replica, id)) 
	key_store.traverse (wrap (this, &dhash::rereplicate_cb));
    }
#endif
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
      chordID final_replica = replicas.back ();
      if (between (m, final_replica, id)) 
	key_store.traverse (wrap (this, &dhash::fix_replicas_cb, id));
    }
#endif

    pred = host_node->my_pred ();
  } else {
    fatal << "update action not supported\n";
  }
  
}

void
dhash::walk_cb(chordID pred, chordID id, chordID k) 
{
  if ((key_status (k) == DHASH_STORED) && (between (pred, id, k))) {
    transfer_key (id, k, DHASH_STORE, wrap(this, &dhash::transfer_key_cb, k));
  }
}

void
dhash::transfer_key_cb (chordID key, dhash_stat err)
{

  if (err == DHASH_OK) {
    dhash_stat status = key_status (key);
    key_store.remove (key);
    key_cache.enter (key, &status);
  } else {
    exit(1);
  }
}

void
dhash::fix_replicas_cb (chordID id, chordID k) 
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
dhash::rereplicate_cb (chordID k) 
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
dhash::transfer_key (chordID to, chordID key, store_status stat, callback<void, dhash_stat>::ref cb) 
{
  return;

  fetch(key, wrap(this, &dhash::transfer_fetch_cb, to, key, stat, cb));
}

void
dhash::transfer_fetch_cb (chordID to, chordID key, store_status stat, callback<void, dhash_stat>::ref cb,
			  ptr<dbrec> data, dhash_stat err) 
{
  
  if (err) {
    cb (err);
  } else {
    unsigned int mtu = 1024;
    dhash_storeres *res = New dhash_storeres ();
    unsigned int off = 0;
    do {
      ptr<dhash_insertarg> i_arg = New refcounted<dhash_insertarg> ();
      i_arg->key = key;
      i_arg->offset = off;
      int remain = (off + mtu <= static_cast<unsigned long>(data->len)) ? mtu : data->len - off;
      i_arg->data.setsize (remain);
      i_arg->attr.size = data->len;
      i_arg->type = stat;
      memcpy(i_arg->data.base () + off, (char *)data->value + off, remain);

      if (stat == DHASH_STORE)
	warn << "going to transfer (STORE) " << key << "(" << off << "/" << data->len << " to " << to << "\n";
      else 
	warn << "going to transfer (REPLICA) " << key << " to " << to << "\n";
 
      host_node->chordnode->doRPC(to, dhash_program_1, DHASHPROC_STORE, i_arg, res,
			     wrap(this, &dhash::transfer_store_cb, cb, res));

      off += remain;
     } while (off < static_cast<unsigned long>(data->len));
  }
  
}

void
dhash::transfer_store_cb (callback<void, dhash_stat>::ref cb, 
			  dhash_storeres *res, clnt_stat err) 
{
  if (err) 
    cb (DHASH_RPCERR);
  else if (res->resok->done)
    cb (res->status);

  delete res;
}

void
dhash::fetch(chordID id, cbvalue cb) 
{
  warnt("DHASH: FETCH_before_db");

  ptr<dbrec> q = id2dbrec(id);
  db->lookup(q, wrap(this, &dhash::fetch_cb, cb));

}

void
dhash::fetch_cb (cbvalue cb, ptr<dbrec> ret) 
{

  warnt("DHASH: FETCH_after_db");

  if (!ret) {
    (*cb)(NULL, DHASH_NOENT);
    warn << "key not found in DB\n";
  } else
    (*cb)(ret, DHASH_OK);
}

void 
dhash::store (dhash_insertarg *arg, cbstore cb)
{

  store_state *ss = pst[arg->key];
  if (arg->data.size () != arg->attr.size) {
    if (!ss) {
      store_state nss (arg->attr.size);
      pst.insert(arg->key, nss);
      ss = pst[arg->key];
    }
    ss->read += arg->data.size ();
    memcpy (ss->buf + arg->offset, arg->data.base (), arg->data.size ());
  }

  if (store_complete(arg)) {
    ptr<dbrec> k = id2dbrec(arg->key);
    ptr<dbrec> d = NULL;
    if (!ss)
      d = New refcounted<dbrec>(arg->data.base (), arg->data.size ());
    else 
      d = New refcounted<dbrec>(ss->buf, ss->size);
    warnt("DHASH: STORE_before_db");
    dhash_stat stat;
    chordID id = arg->key;
    if (arg->type == DHASH_STORE) {
      stat = DHASH_STORED;
      key_store.enter (id, &stat);
    } else if (arg->type == DHASH_REPLICA) {
      stat = DHASH_REPLICATED;
      key_store.enter (id, &stat);
    } else {
      stat = DHASH_CACHED;
      key_cache.enter (id, &stat);
    }
    
    db->insert (k, d, wrap(this, &dhash::store_cb, arg->type, id, cb));

  } else
    cb (DHASH_STORE_PARTIAL);

}

bool
dhash::store_complete (dhash_insertarg *arg) 
{
  if (arg->data.size () == arg->attr.size) return true;
  store_state *ss = pst[arg->key];
  if (!ss) return false;
  else
    return (ss->read >= arg->attr.size);
}

void
dhash::store_cb(store_status type, chordID id, cbstore cb, int stat) 
{
  warnt("DHASH: STORE_after_db");

  if (stat) warn << "DB3 gave the error : " << stat << "\n";
  if (stat != 0) 
    (*cb)(DHASH_STOREERR);
  //  else if (type == DHASH_STORE)
  // replicate_key (id, REP_DEGREE, wrap (this, &dhash::store_repl_cb, cb));
  else	   
    (*cb)(DHASH_OK);
  
  store_state *ss = pst[id];
  if (ss) {
    if (ss) delete ss->buf;
    pst.remove (id);
  }
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
dhash::replicate_key (chordID key, int degree, callback<void, dhash_stat>::ref cb) 
{
  return;
  warnt ("DHASH: replicate_key");
  if (degree > 0) {
    chordID succ = host_node->my_succ ();
    vec<chordID> repls;
    transfer_key (succ, key, DHASH_REPLICA, wrap (this, &dhash::replicate_key_transfer_cb,
						  key, degree, cb, succ, repls));
  } else
    cb (DHASH_OK);
}

void
dhash::replicate_key_succ_cb (chordID key, int degree_remaining, 
			      callback<void, dhash_stat>::ref cb,
			      vec<chordID> repls,
			      chordID succ, chordstat err) 
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
dhash::replicate_key_transfer_cb (chordID key, int degree_remaining, callback<void, dhash_stat>::ref cb,
				  chordID succ, vec<chordID> repls,
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
    host_node->get_succ (succ, wrap (this, &dhash::replicate_key_succ_cb, 
				key, 
				degree_remaining - 1,
				cb, repls));
  }
}

// --------- utility

ptr<dbrec>
dhash::id2dbrec(chordID id) 
{
  str whipme = id.getraw ();
  void *key = (void *)whipme.cstr ();
  int len = whipme.len ();
  
  ptr<dbrec> q = New refcounted<dbrec> (key, len);
  return q;
}

dhash_stat
dhash::key_status(chordID n) {
  dhash_stat * s_stat = key_store.peek (n);
  if (s_stat != NULL)
    return *s_stat;
  
  dhash_stat * c_stat = key_cache.peek (n);
  if (c_stat != NULL)
    return *c_stat;

  return DHASH_NOTPRESENT;
}

char
dhash::responsible(chordID& n) 
{
  chordID p = host_node->my_pred ();
  chordID m = host_node->my_ID ();
  return (between (p, m, n));
}

void
dhash::store_flush (chordID key, dhash_stat value) {
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
dhash::cache_flush (chordID key, dhash_stat value) {
  warn << "flushing element " << key << " from cache\n";
  ptr<dbrec> k = id2dbrec(key);
  db->del (k, wrap(this, &dhash::cache_flush_cb));
}

void
dhash::cache_flush_cb (int err) {
  if (err) warn << "err flushing from cache\n";
}

// - RPC 

void
dhash::dhash_reply (long xid, unsigned long procno, void *res) 
{

  xdrproc_t proc = dhash_program_1.tbl[procno].xdr_res;
  assert (proc);

  xdrsuio x (XDR_ENCODE);
  if (!proc (x.xdrp (), static_cast<void *> (res))) {
    warn << "failed to marshall result\n";
    assert (0);
  }

  size_t marshalled_len = x.uio ()->resid ();
  char *marshalled_data = suio_flatten (x.uio ());

  host_node->chordnode->locations->reply(xid, marshalled_data, 
					 marshalled_len);
  delete marshalled_data;
}

// ---------- debug ----
void
dhash::printkeys () 
{
  warn << "ID: " << host_node->my_ID () << "\n";
  key_store.traverse (wrap (this, &dhash::printkeys_walk));
  key_cache.traverse (wrap (this, &dhash::printcached_walk));
}

void
dhash::printkeys_walk (chordID k) 
{
  warn << k << " STORED\n";
}

void
dhash::printcached_walk (chordID k) 
{
  warn << k << " CACHED\n";
}
