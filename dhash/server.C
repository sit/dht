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


dhash::dhash(str dbname, vnode *node, int k, int ss, int cs) :
  host_node (node), key_store(ss), key_cache(cs) {

  db = New dbfe();
  nreplica = k;
  rc_delay = 7;
  kc_delay = 11;

  //set up the options we want
  dbOptions opts;
  opts.addOption("opt_async", 1);
  opts.addOption("opt_cachesize", 80000);
  opts.addOption("opt_nodesize", 4096);

  if (int err = db->opendb(const_cast < char *>(dbname.cstr()), opts)) {
    warn << "open returned: " << strerror(err) << err << "\n";
    exit (-1);
  }
  
  assert(host_node);

  key_store.set_flushcb (wrap (this, &dhash::store_flush));
  key_replicate.set_flushcb (wrap (this, &dhash::store_flush));  
  key_cache.set_flushcb (wrap (this, &dhash::cache_flush));

  /* statistics */
  keys_stored = 0;
  bytes_stored = 0;
  bytes_served = 0;
  keys_replicated = 0;
  keys_cached = 0;

  //  init_key_status ();
  update_replica_list ();
  install_replica_timer ();
  install_keycheck_timer ();
  transfer_initial_keys ();

  // RPC demux
  host_node->addHandler (DHASH_PROGRAM, wrap(this, &dhash::dispatch));
}

void
dhash::dispatch (unsigned long procno, 
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

  case DHASHPROC_FETCHITER:
    {
      warnt ("DHASH: fetchiter_request");

      dhash_fetch_arg *farg = New dhash_fetch_arg ();
      if (!proc (x.xdrp (), farg)) {
	warn << "DHASH: error unmarshalling arguments\n";
	return;
      }

      dhash_fetchiter_res *res = New dhash_fetchiter_res (DHASH_CONTINUE);

      if (key_status (farg->key) != DHASH_NOTPRESENT) {
	//fetch the key and return it, end of story
	fetch (farg->key, wrap (this, &dhash::fetchiter_svc_cb,
				rpc_id, farg));
	delete res;
	return;
      } else if (responsible (farg->key))  {
	//no where else to go, return NOENT or RETRY?
	res->set_status (DHASH_NOENT);
      } else {
	res->set_status (DHASH_CONTINUE);
	chordID nid;
	chordID myID = host_node->my_ID ();
	chordID my_succ = host_node->my_succ ();
	if (betweenrightincl(myID, my_succ, farg->key))
	  nid = my_succ;
	else
	  nid = host_node->lookup_closestpred (farg->key);
	
	res->cont_res->next.x = nid;
	res->cont_res->next.r = 
	  host_node->chordnode->locations->getaddress (nid);

	res->cont_res->succ_list.setsize (NSUCC);

	chordID last = nid;
	for (int i = 0; i < NSUCC; i++) {
	  chord_node nsucc;
	  nsucc.x = host_node->lookup_closestsucc (last);
	  nsucc.r = host_node->chordnode->locations->getaddress (nsucc.x);
	  res->cont_res->succ_list[i] = nsucc;
	  last = nsucc.x;
	}

      }

      dhash_reply (rpc_id, DHASHPROC_FETCHITER, res);
      delete res;
      delete farg;
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
	dhash_storeres *res = New dhash_storeres (DHASH_RETRY);
	chordID pred = host_node->my_pred ();
	res->pred->p.x = pred;
	res->pred->p.r = host_node->chordnode->locations->getaddress (pred);
	dhash_reply (rpc_id, DHASHPROC_STORE, res);
      } else {
	warnt("DHASH: will store");
	store(sarg, wrap(this, &dhash::storesvc_cb, rpc_id, sarg));	
      }
      
    }
    break;
  case DHASHPROC_GETKEYS:
    {
      dhash_getkeys_arg *gkarg = New dhash_getkeys_arg ();
      if (!proc (x.xdrp (), gkarg)) {
	warn << "DHASH: error unmarshalling arguments (getkey)\n";
	return;
      }
      
      dhash_getkeys_res *res = New dhash_getkeys_res (DHASH_OK);
      ref<vec<chordID> > keys = New refcounted<vec<chordID> >;
      key_store.traverse (wrap (this, &dhash::get_keys_traverse_cb, keys, 
				gkarg->pred_id));

      res->resok->keys.set (keys->base (), keys->size (), freemode::NOFREE);
      dhash_reply (rpc_id, DHASHPROC_GETKEYS, res);
      delete res;
      delete gkarg;
    }
    break;
  case DHASHPROC_KEYSTATUS:
    {
      chordID *arg = New chordID;
      if (!proc (x.xdrp (), arg)) {
	warn << "DHASH: error unmarshalling arguments (keystatus)\n";
	return;
      }

      //return the status of this key (needed?)
      dhash_stat stat = key_status (*arg);
      dhash_reply (rpc_id, DHASHPROC_KEYSTATUS, &stat);
      delete arg;
    }
    break;
  default:
    break;
  }
  pred = host_node->my_pred ();

  return;
}

void
dhash::fetchiter_svc_cb (long xid, dhash_fetch_arg *arg,
			 ptr<dbrec> val, dhash_stat err) 
{
  dhash_fetchiter_res *res = New dhash_fetchiter_res (DHASH_CONTINUE);
  if (err) res->set_status (DHASH_NOENT);
  else {
    res->set_status (DHASH_COMPLETE);
  
    int n = (arg->len + arg->start < val->len) ? 
      arg->len : val->len - arg->start;

    res->compl_res->res.setsize (n);
    res->compl_res->attr.size = val->len;
    res->compl_res->offset = arg->start;
    res->compl_res->source = host_node->my_ID ();

    memcpy (res->compl_res->res.base (), 
	    (char *)val->value + arg->start, 
	    n);
    /* statistics */
    bytes_served += n;
  }
  
  dhash_reply (xid, DHASHPROC_FETCHITER, res);
  delete res;
  delete arg;
}

void
dhash::fetchsvc_cb (long xid,
		    dhash_fetch_arg *arg, 
		    ptr<dbrec> val, 
		    dhash_stat err)
{
  dhash_res *res = New dhash_res (DHASH_OK);

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
  } else {
    res->set_status (DHASH_NOENT);
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

  dhash_storeres *res = New dhash_storeres (DHASH_OK);
  if ((err != DHASH_OK) && (err != DHASH_STORE_PARTIAL)) 
    res->set_status (err);
  else {
    res->resok->source = host_node->my_ID ();
    res->resok->done = (err == DHASH_OK);
  }

  dhash_reply (xid, DHASHPROC_STORE, res);
  delete res;
  delete arg;
}

void
dhash::get_keys_traverse_cb (ptr<vec<chordID> > vKeys,
			     chordID predid,
			     chordID key)
{
  if (key < predid) 
    vKeys->push_back (key);
}

//---------------- no sbp's below this line --------------
 
// -------- reliability stuff

void
dhash::init_key_status () 
{
  warn << "EXAMINING DATABASE\n";
  dhash_stat c = DHASH_CACHED;
  dhash_stat s = DHASH_STORED;
  /* probably not safe if I ever fix ADB */
  ptr<dbEnumeration> it = db->enumerate();
  ptr<dbPair> d = it->nextElement();
  while (d) {
    chordID k = dbrec2id (d->key);
    if (responsible (k)) 
      key_store.enter (k, &s);
    else
      key_store.enter (k, &c);

    warn << "found " << k << " in database\n";
    d = it->nextElement();
  } 
}

void
dhash::transfer_initial_keys ()
{
  chordID succ = host_node->my_succ ();

  ptr<dhash_getkeys_arg> arg = New refcounted<dhash_getkeys_arg>;
  arg->pred_id = host_node->my_ID ();
  
  dhash_getkeys_res *res = New dhash_getkeys_res (DHASH_OK);
  host_node->chordnode->doRPC(succ, dhash_program_1, DHASHPROC_GETKEYS, 
			      arg, res,
			      wrap(this, 
				   &dhash::transfer_init_getkeys_cb, res));
}

void
dhash::transfer_init_getkeys_cb (dhash_getkeys_res *res, clnt_stat err)
{
  
  if ((err) || (res->status != DHASH_OK)) 
    fatal << "Couldn't transfer keys from my successor\n";

  chordID succ = host_node->my_succ ();
  for (unsigned int i = 0; i < res->resok->keys.size (); i++) {
    get_key (succ, res->resok->keys[i], 
	     wrap (this, &dhash::transfer_init_gotk_cb));
    warn << res->resok->keys[i] << "\n";
  }
  delete res;
}

void
dhash::transfer_init_gotk_cb (dhash_stat err) 
{
  if (err) fatal << "Error fetching key\n";
}

void
dhash::update_replica_list () 
{
  replicas.clear ();
  for (int i = 0; i < nreplica; i++)
    replicas.push_back(host_node->nth_successorID (i));
}

void
dhash::install_replica_timer () 
{
  check_replica_tcb = delaycb (rc_delay, 0, 
			       wrap (this, &dhash::check_replicas_cb));
};

void 
dhash::install_keycheck_timer () 
{
  check_key_tcb = delaycb (kc_delay, 0, 
			   wrap (this, &dhash::check_keys_timer_cb));
};

/* O( (number of replicas)^2 ) (but doesn't assume anything about
ordering of chord::succlist*/
bool 
dhash::isReplica(chordID id) { 
  for (unsigned int i=0; i < replicas.size(); i++)
    if (replicas[i] == id) return true;
  return false;
}

void
dhash::check_replicas_cb () {
  check_replicas ();
  install_replica_timer ();
}

void
dhash::check_replicas () 
{
  for (int i=0; i < nreplica; i++) {
    chordID nth = host_node->nth_successorID (i);
    if (!isReplica(nth)) 
      key_store.traverse (wrap (this, &dhash::check_replicas_traverse_cb, 
				nth));
  }
  update_replica_list ();
}

void
dhash::check_replicas_traverse_cb (chordID to, chordID key)
{
  if (key_status (key) == DHASH_STORED)
    transfer_key (to, key, DHASH_REPLICA, wrap (this, 
						&dhash::fix_replicas_txerd));
}

void
dhash::fix_replicas_txerd (dhash_stat err) 
{
  if (err) warn << "error replicating key\n";
}

void
dhash::check_keys_timer_cb () 
{
  // printkeys ();
  key_store.traverse (wrap (this, &dhash::check_keys_traverse_cb));
  key_replicate.traverse (wrap (this, &dhash::check_keys_traverse_cb));
  key_cache.traverse (wrap (this, &dhash::check_keys_traverse_cb));
  install_keycheck_timer ();
}

void
dhash::check_keys_traverse_cb (chordID key) 
{
  if ( (responsible (key)) && (key_status(key) != DHASH_STORED)) {
    change_status (key, DHASH_STORED);
  } else if ( (!responsible (key) && (key_status (key) == DHASH_STORED))) {
    change_status (key, DHASH_REPLICATED);
  }

}
// --- node to node transfers ---
void
dhash::replicate_key (chordID key, cbstat_t cb)
{
  if (replicas.size () > 0) {
    location *l = host_node->chordnode->locations->getlocation (replicas[0]);
    if (!l) {
      check_replicas_cb ();
      replicate_key (key, cb);
      return;
    }
    transfer_key (replicas[0], key, DHASH_REPLICA, 
		  wrap (this, &dhash::replicate_key_cb, 1, cb, key));
  }
  else
    (cb)(DHASH_OK);
}

void
dhash::replicate_key_cb (unsigned int replicas_done, cbstat_t cb, chordID key,
			 dhash_stat err) 
{
  if (err) (*cb)(DHASH_ERR);
  else if (replicas_done >= replicas.size ()) (*cb)(DHASH_OK);
  else {
    location *l = host_node->chordnode->locations->getlocation (replicas[replicas_done]);
    if (!l) {
      check_replicas_cb ();
      replicate_key (key, cb);
      return;
    }
    transfer_key (replicas[replicas_done], key, DHASH_REPLICA,
		  wrap (this, &dhash::replicate_key_cb, 
			replicas_done + 1, cb, key));
  }
						
}
void
dhash::transfer_key (chordID to, chordID key, store_status stat, 
		     callback<void, dhash_stat>::ref cb) 
{
  fetch(key, wrap(this, &dhash::transfer_fetch_cb, to, key, stat, cb));
}

void
dhash::transfer_fetch_cb (chordID to, chordID key, store_status stat, 
			  callback<void, dhash_stat>::ref cb,
			  ptr<dbrec> data, dhash_stat err) 
{
  
  if (err) {
    cb (err);
  } else {
    unsigned int mtu = 1024;
    unsigned int off = 0;
    do {
      dhash_storeres *res = New dhash_storeres (DHASH_OK);
      ptr<dhash_insertarg> i_arg = New refcounted<dhash_insertarg> ();
      i_arg->key = key;
      i_arg->offset = off;
      int remain = (off + mtu <= static_cast<unsigned long>(data->len)) ? 
	mtu : data->len - off;
      i_arg->data.setsize (remain);
      i_arg->attr.size = data->len;
      i_arg->type = stat;
      memcpy(i_arg->data.base (), (char *)data->value + off, remain);
 
      host_node->chordnode->doRPC(to, dhash_program_1, DHASHPROC_STORE, 
				  i_arg, res,
				  wrap(this, 
				       &dhash::transfer_store_cb, cb, 
				       res, i_arg, to));

      off += remain;
     } while (off < static_cast<unsigned long>(data->len));
  }
  
}

void
dhash::transfer_store_cb (callback<void, dhash_stat>::ref cb, 
			  dhash_storeres *res, ptr<dhash_insertarg> i_arg,
			  chordID to, clnt_stat err) 
{
  if (err) 
    cb (DHASH_RPCERR);
  else if (res->status == DHASH_RETRY) {
    dhash_storeres *nres = New dhash_storeres (DHASH_OK);
    host_node->chordnode->locations->cacheloc (res->pred->p.x, 
					       res->pred->p.r);
					       
    host_node->chordnode->doRPC(res->pred->p.x, 
				dhash_program_1, DHASHPROC_STORE, 
				i_arg, nres,
				wrap(this, 
				     &dhash::transfer_store_cb, 
				     cb, nres, i_arg,
				     res->pred->p.x));
  } else if (res->resok->done)
    cb (res->status);

  delete res;
}

void
dhash::get_key (chordID source, chordID key, cbstat_t cb) 
{
  dhash_res *res = New dhash_res (DHASH_OK);
  ptr<dhash_fetch_arg> arg = New refcounted<dhash_fetch_arg>;
  arg->key = key; 
  arg->len = MTU;  
  arg->start = 0;

  host_node->chordnode->doRPC(source, dhash_program_1, DHASHPROC_FETCH, 
			      arg, res,
			      wrap(this, 
				   &dhash::get_key_initread_cb, 
				   cb, res, source, key));
}

void
dhash::get_key_initread_cb (cbstat_t cb, dhash_res *res, chordID source, 
			    chordID key, clnt_stat err) 
{
  if ((err) || (res->status != DHASH_OK)) {
    (cb)(DHASH_RPCERR);
  } else if (res->resok->attr.size == res->resok->res.size ()) {
    get_key_finish (res->resok->res.base (), res->resok->res.size (), 
		    key, cb);
  } else {
    char *buf = New char[res->resok->attr.size];
    assert (buf);
    memcpy (buf, res->resok->res.base (), res->resok->res.size ());
    unsigned int *read = New unsigned int(res->resok->res.size ());
    unsigned int offset = *read;
    do {
      ptr<dhash_fetch_arg> arg = New refcounted<dhash_fetch_arg>;
      arg->key = key;
      arg->len = (offset + MTU < res->resok->attr.size) ? 
	MTU : res->resok->attr.size - offset;
      arg->start = offset;
      dhash_res *new_res = New dhash_res(DHASH_OK);
      host_node->chordnode->doRPC(source, dhash_program_1, DHASHPROC_FETCH, 
				  arg, new_res,
				  wrap(this, 
				       &dhash::get_key_read_cb, key, 
				       buf, read, new_res, cb));
      offset += arg->len;
    } while (offset  < res->resok->attr.size);
  }
  
  delete res;
}

void
dhash::get_key_read_cb (chordID key, char *buf, unsigned int *read, 
			dhash_res *res, cbstat_t cb, clnt_stat err) 
{
  if ( (err) || (res->status != DHASH_OK)) {
    delete buf;
    (*cb)(DHASH_RPCERR);
  } else {
    *read += res->resok->res.size ();
    memcpy (buf + res->resok->offset, res->resok->res.base (), 
	    res->resok->res.size ());
    if (*read == res->resok->attr.size) {
      get_key_finish (buf, res->resok->res.size (), key, cb);
      delete buf;
      delete res;
    }
  }
  delete res;
}

void
dhash::get_key_finish (char *buf, unsigned int size, chordID key, cbstat_t cb) 
{
  ptr<dbrec> k = id2dbrec (key);
  ptr<dbrec> d = New refcounted<dbrec> (buf, size);
  dhash_stat stat = DHASH_STORED;
  key_store.enter (key, &stat);
  db->insert (k, d, wrap(this, &dhash::get_key_finish_store, cb));
}

void
dhash::get_key_finish_store (cbstat_t cb, int err)
{
  if (err)
    (cb)(DHASH_STOREERR);
  else
    (cb)(DHASH_OK);
}


// --- node to database transfers --- 

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
      keys_stored++;
      stat = DHASH_STORED;
      key_store.enter (id, &stat);
    } else if (arg->type == DHASH_REPLICA) {
      keys_replicated++;
      stat = DHASH_REPLICATED;
      key_replicate.enter (id, &stat);
    } else {
      keys_cached++;
      stat = DHASH_CACHED;
      key_cache.enter (id, &stat);
    }
    
    db->insert (k, d, wrap(this, &dhash::store_cb, arg->type, id, cb));

    /* statistics */
    if (ss) bytes_stored += ss->size;
    else bytes_stored += arg->data.size ();
    
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
  else if (type == DHASH_STORE)
    replicate_key (id,  wrap (this, &dhash::store_repl_cb, cb));
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
  if (err) cb (err);
  else cb (DHASH_OK);
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

chordID
dhash::dbrec2id (ptr<dbrec> r) {
  str raw = str ( (char *)r->value, r->len);
  chordID ret;
  ret.setraw (raw);
  return ret;
}
void
dhash::change_status (chordID key, dhash_stat newstat) 
{

  if (key_status (key) == DHASH_STORED) 
    key_store.remove (key);
  else if (key_status (key) == DHASH_REPLICATED)
    key_replicate.remove (key);
  else
    key_cache.remove (key);

  if (newstat == DHASH_STORED)
    key_store.enter (key, &newstat);
  if (newstat == DHASH_REPLICATED)
    key_replicate.enter (key, &newstat);
  else if (newstat == DHASH_CACHED)
    key_cache.enter (key, &newstat);

}

dhash_stat
dhash::key_status(chordID n) 
{
  dhash_stat * s_stat = key_store.peek (n);
  if (s_stat != NULL)
    return *s_stat;

  s_stat = key_replicate.peek (n);
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
  store_state *ss = pst[n];
  if (ss) return true; //finish any store we start
  chordID p = host_node->my_pred ();
  chordID m = host_node->my_ID ();
  return (between (p, m, n));
}

void
dhash::store_flush (chordID key, dhash_stat value) {
  warn << "flushing element " << key << " from store\n";
  ptr<dbrec> k = id2dbrec(key);
  dhash_stat c = DHASH_CACHED;
  key_cache.enter (key, &c);
  db->del (k, wrap(this, &dhash::store_flush_cb));
}
 
void
dhash::store_flush_cb (int err) {
  if (err) warn << "Error removing element\n";
}

void
dhash::cache_flush (chordID key, dhash_stat value) {

  if (key_status (key) != DHASH_CACHED) return;
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
  key_store.traverse (wrap (this, &dhash::printkeys_walk));
  key_replicate.traverse (wrap (this, &dhash::printkeys_walk));
  key_cache.traverse (wrap (this, &dhash::printcached_walk));
}

void
dhash::printkeys_walk (chordID k) 
{
  dhash_stat status = key_status (k);
  if (status == DHASH_STORED)
    warn << k << " STORED\n";
  else if (status == DHASH_REPLICATED)
    warn << k << " REPLICATED\n";
  else
    warn << k << " UNKNOWN\n";
}

void
dhash::printcached_walk (chordID k) 
{
  warn << k << " CACHED\n";
}

void
dhash::print_stats () 
{
  warnx << "ID: " << host_node->my_ID () << "\n";
  warnx << "Stats:\n";
  warnx << "  " << keys_stored << " stored\n";
  warnx << "  " << keys_cached << " keys cached\n";
  warnx << "  " << keys_replicated << " keys replicated\n";
  warnx << "  " << bytes_stored << " total bytes held\n";
    
}

