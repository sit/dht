#include <dhash.h>
#include <dhash_prot.h>
#include <chord.h>
#include <chord_prot.h>
#include <chord_util.h>
#include <dbfe.h>
#include <arpc.h>
#include <dmalloc.h>


dhash::dhash(str dbname, vnode *node, int k, int ss, int cs, int _ss_mode) :
  ss_mode (_ss_mode / 10), host_node (node), key_store(ss), key_cache(cs) {

  db = New dbfe();
  nreplica = k;
  rc_delay = 7;
  kc_delay = 11;
  
  //recursive state
  qnonce = 1;

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

  case DHASHPROC_FETCHITER:
    {
      warnt ("DHASH: fetchiter_request");

      dhash_fetch_arg *farg = New dhash_fetch_arg ();
      if (!proc (x.xdrp (), farg)) {
	warn << "DHASH: (fetch) error unmarshalling arguments\n";
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
	
	vec <chord_node> s_list;
	chordID last = nid;
	chord_node nsucc;
	nsucc.x = nid;
	nsucc.r = host_node->chordnode->locations->getaddress (nsucc.x);
	s_list.push_back (nsucc);
	for (int i = 0; i < NSUCC; i++) {
	  chordID next = host_node->lookup_closestsucc (last);
	  if (next == s_list[0].x) break;
	  nsucc.x = next;
	  nsucc.r = host_node->chordnode->locations->getaddress (nsucc.x);
	  s_list.push_back (nsucc);
	  last = nsucc.x;
	}
	
	res->cont_res->succ_list.setsize (s_list.size ());
	for (unsigned int i = 0; i < s_list.size (); i++)
	  res->cont_res->succ_list[i] = s_list[i];

	chordID best_succ = res->cont_res->succ_list[0].x;
	
	if ((ss_mode > 0) && (nid == my_succ)) {
	  //returning a node which will hold the key, pick the fastest
	  locationtable *locations = host_node->chordnode->locations;
	  location *c = locations->getlocation (best_succ);
	  location *n;
	  int lim = ((int)s_list.size () < nreplica) ? 
	    s_list.size ():
	    nreplica;
	  for (int i = 1; i < lim; i++) {
	    n = locations->getlocation(res->cont_res->succ_list[i].x);
	    if (n->nrpc == 0) continue;
	    if ((c->nrpc == 0) || 
		(n->rpcdelay/n->nrpc) < (c->rpcdelay/c->nrpc)) {
	      c = n;
	      best_succ = res->cont_res->succ_list[i].x;
	    }
	  }
	}

	res->cont_res->next.x = best_succ;
	res->cont_res->next.r = 
	  host_node->chordnode->locations->getaddress (best_succ);
      }
      
      dhash_reply (rpc_id, DHASHPROC_FETCHITER, res);
      delete res;
      delete farg;
      
    }
    break;
    
  case DHASHPROC_FETCHRECURS:
    {

      dhash_recurs_arg *rarg = New dhash_recurs_arg ();
      if (!proc (x.xdrp (), rarg)) {
	warn << "DHASH: (fetchrecurs) error unmarshalling arguments\n";
	return;
      }

      // if this is a new query, make us the return address and hold on to sbp
      int nonce = rarg->nonce;
      chord_node return_address = rarg->return_address;
      if (nonce == 0) {
	nonce = qnonce++;
	rqc.insert (nonce, rpc_id);
	return_address.x = host_node->my_ID ();
	return_address.r = 
	  host_node->chordnode->locations->getaddress (return_address.x);
      }

      if (key_status (rarg->key) != DHASH_NOTPRESENT) {
	// if we have the data, send an RPC to the return address
	fetch (rarg->key, wrap (this, &dhash::fetchrecurs_havedata_cb,
				rpc_id, rarg, nonce, return_address));

	return;
      } else if (responsible (rarg->key))  {
	//we should have the key, send an error RPC to the return address
	
	ptr<dhash_finish_recurs_arg> arg = 
	  New refcounted<dhash_finish_recurs_arg>;
	
	arg->nonce = nonce;
	arg->status = DHASH_NOENT;
	arg->source.x = host_node->my_ID ();
	arg->source.r = 
	  host_node->chordnode->locations->getaddress (arg->source.x);
	
	dhash_stat *err_res = New dhash_stat; 
	host_node->chordnode->locations->cacheloc (return_address.x, 
						   return_address.r);
	doRPC (return_address.x, dhash_program_1, DHASHPROC_FINISHRECURS, 
	       arg, err_res,
	       wrap (this, &dhash::fetchrecurs_sent_data, err_res));
      } else {
	// if we don't, are we the predecessor?
	chordID nid;
	chordID myID = host_node->my_ID ();
	chordID my_succ = host_node->my_succ ();
	if (betweenrightincl(myID, my_succ, rarg->key))
	  nid = my_succ;
	else 	// nope, look up the next best pred
	  nid = host_node->lookup_closestpred (rarg->key);
	
	ptr<dhash_recurs_arg> arg = New refcounted<dhash_recurs_arg> ();
	arg->key = rarg->key;
	arg->start = rarg->start;
	arg->len = rarg->len;
	arg->hops = rarg->hops + 1;
	arg->return_address = return_address;
	arg->nonce = nonce;

	chordID best = nid;
	if ( (ss_mode > 0) && (nid == my_succ)) {
	  location *b = host_node->chordnode->locations->getlocation (nid);
	  chordID last = nid;
	  for (int i = 0; i < nreplica; i++) {
	    chordID next = host_node->lookup_closestsucc (last);
	    location *n = host_node->chordnode->locations->getlocation (last);
	    if (n->nrpc == 0) continue;
	    if (b->nrpc == 0) continue;
	    double n_lat = (double)n->rpcdelay / n->nrpc;
	    double b_lat = (double)b->rpcdelay / b->nrpc;
	    if (n_lat + 50000 < b_lat) {
	      b = n;
	      best = next;
	    }
	    last = next;
	  }
	}
	dhash_fetchrecurs_res *c_res = New dhash_fetchrecurs_res ();
	doRPC (best, dhash_program_1, DHASHPROC_FETCHRECURS, arg, c_res,
	       wrap (this, &dhash::fetchrecurs_continue, c_res));
      }

      dhash_stat rres = DHASH_OK;
      if (rarg->nonce)
	dhash_reply (rpc_id, DHASHPROC_FETCHRECURS, &rres);
      delete rarg;
    }
    break;
  case DHASHPROC_FINISHRECURS:
    {
      dhash_finish_recurs_arg *farg = New dhash_finish_recurs_arg ();
      if (!proc (x.xdrp (), farg)) {
	warn << "DHASH: (fetchrecurs) error unmarshalling arguments\n";
	return;
      }
      
      dhash_fetchrecurs_res *res = New dhash_fetchrecurs_res ();
      unsigned long *srpc_id = rqc[farg->nonce];
      assert (srpc_id != NULL);
      if (farg->status != DHASH_RFETCHDONE) res->set_status (farg->status);
      else {

	//cache loc since we will probably do a TRANSFER to it next
	host_node->chordnode->locations->cacheloc (farg->source.x, 
						   farg->source.r);

	res->set_status (DHASH_RFETCHDONE);
	res->compl_res->res.setsize (farg->data.res.size());
	memcpy (res->compl_res->res.base (), farg->data.res.base (), 
		res->compl_res->res.size ());
	res->compl_res->offset = farg->data.offset;
	res->compl_res->attr.size = farg->data.attr.size;
	res->compl_res->hops = farg->hops;
	res->compl_res->source = farg->data.source;
	
      }
      dhash_reply (*srpc_id, DHASHPROC_FETCHRECURS, res);
      delete res;

      dhash_stat stat = DHASH_OK;
      dhash_reply (rpc_id, DHASHPROC_FINISHRECURS, &stat);
      delete farg;
    }
    break;
  case DHASHPROC_STORE:
    {
      update_replica_list ();
      warnt("DHASH: STORE_request");

      dhash_insertarg *sarg = New dhash_insertarg ();
      if (!proc (x.xdrp (), sarg)) {
	warn << "DHASH: (store) error unmarshalling arguments\n";
	return;
      }
     
      if ((sarg->type == DHASH_STORE) && 
	  (!responsible (sarg->key)) && 
	  (!pst[sarg->key])) {
	warnt("DHASH: retry");
	warn << "RETRY for " << sarg->key << "\n";
	dhash_storeres *res = New dhash_storeres (DHASH_RETRY);
	chordID pred = host_node->my_pred ();
	res->pred->p.x = pred;
	res->pred->p.r = host_node->chordnode->locations->getaddress (pred);
	dhash_reply (rpc_id, DHASHPROC_STORE, res);
      } else {
	warnt ("DHASH: will store");
	store (sarg, wrap(this, &dhash::storesvc_cb, rpc_id, sarg));	
      }
      
    }
    break;
  case DHASHPROC_GETKEYS:
    {
      dhash_getkeys_arg *gkarg = New dhash_getkeys_arg ();
      if (!proc (x.xdrp (), gkarg)) {
	warn << "DHASH: (getkeys) error unmarshalling arguments (getkey)\n";
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
	warn << "DHASH: (keystatus) error unmarshalling arguments (keystatus)\n";
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
dhash::fetchrecurs_continue (dhash_fetchrecurs_res *res, clnt_stat err) 
{
  if (err) warn << "frecurs_continue: err\n";
  else 
    assert (res->status != DHASH_RFETCHFORWARDED) ;
  delete res;
      
}
void
dhash::fetchrecurs_havedata_cb (unsigned long rpc_id,
				dhash_recurs_arg *rarg,
				unsigned int nonce,
				chord_node return_address,
				ptr<dbrec> val, dhash_stat err)
{
  
  ptr<dhash_finish_recurs_arg> res_arg = 
    New refcounted<dhash_finish_recurs_arg> ();
  
  res_arg->nonce = nonce;
  res_arg->status = DHASH_RFETCHDONE;
  chordID myID = host_node->my_ID ();
  res_arg->source.r = host_node->chordnode->locations->getaddress (myID);
  res_arg->source.x = host_node->my_ID ();
  res_arg->hops = rarg->hops + 1;
  host_node->chordnode->locations->cacheloc (return_address.x, 
					     return_address.r);
  
  int n = (rarg->len + rarg->start < val->len) ? 
    rarg->len : val->len - rarg->start;
  
  res_arg->data.res.setsize (n);
  res_arg->data.attr.size = val->len;
  res_arg->data.offset = rarg->start;
  res_arg->data.source = host_node->my_ID ();
  
  memcpy (res_arg->data.res.base (), 
	  (char *)val->value + rarg->start, 
	  n);

  /* statistics */
  bytes_served += n;
  
  dhash_stat *done_res = New dhash_stat;
  doRPC (return_address.x, dhash_program_1, DHASHPROC_FINISHRECURS, 
	 res_arg, done_res,
	 wrap (this, &dhash::fetchrecurs_sent_data, done_res));

  dhash_stat rres = DHASH_OK;
  if (rarg->nonce)
    dhash_reply (rpc_id, DHASHPROC_FETCHRECURS, &rres);
  delete rarg;
}

void
dhash::fetchrecurs_sent_data (dhash_stat *done_res, clnt_stat err)
{
  if (err || (*done_res != DHASH_OK)) warn << "fr_sent_data: error\n";
  delete done_res;
}

void
dhash::fetchiter_svc_cb (long xid, dhash_fetch_arg *arg,
			 ptr<dbrec> val, dhash_stat err) 
{
  dhash_fetchiter_res *res = New dhash_fetchiter_res (DHASH_CONTINUE);
  if (err) 
    res->set_status (DHASH_NOENT);
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
  doRPC(succ, dhash_program_1, DHASHPROC_GETKEYS, 
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
  for (int i = 1; i < nreplica+1; i++) 
    if (host_node->nth_successorID (i) != host_node->my_ID ())
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
  for (unsigned int i=0; i < replicas.size (); i++) {
    chordID nth = host_node->nth_successorID (i+1);
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
 
      doRPC(to, dhash_program_1, DHASHPROC_STORE, 
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
					       
    doRPC(res->pred->p.x, 
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
  dhash_fetchiter_res *res = New dhash_fetchiter_res (DHASH_OK);
  ptr<dhash_fetch_arg> arg = New refcounted<dhash_fetch_arg>;
  arg->key = key; 
  arg->len = MTU;  
  arg->start = 0;

  doRPC(source, dhash_program_1, DHASHPROC_FETCHITER, 
			      arg, res,
			      wrap(this, 
				   &dhash::get_key_initread_cb, 
				   cb, res, source, key));
}

void
dhash::get_key_initread_cb (cbstat_t cb, dhash_fetchiter_res *res, 
			    chordID source, 
			    chordID key, clnt_stat err) 
{
  if ((err) || (res->status != DHASH_COMPLETE)) {
    (cb)(DHASH_RPCERR);
  } else if (res->compl_res->attr.size == res->compl_res->res.size ()) {
    get_key_finish (res->compl_res->res.base (), res->compl_res->res.size (), 
		    key, cb);
  } else {
    char *buf = New char[res->compl_res->attr.size];
    assert (buf);
    memcpy (buf, res->compl_res->res.base (), res->compl_res->res.size ());
    unsigned int *read = New unsigned int(res->compl_res->res.size ());
    unsigned int offset = *read;
    do {
      ptr<dhash_fetch_arg> arg = New refcounted<dhash_fetch_arg>;
      arg->key = key;
      arg->len = (offset + MTU < res->compl_res->attr.size) ? 
	MTU : res->compl_res->attr.size - offset;
      arg->start = offset;
      dhash_fetchiter_res *new_res = New dhash_fetchiter_res(DHASH_OK);
      doRPC(source, dhash_program_1, DHASHPROC_FETCHITER,
				  arg, new_res,
				  wrap(this, 
				       &dhash::get_key_read_cb, key, 
				       buf, read, new_res, cb));
      offset += arg->len;
    } while (offset  < res->compl_res->attr.size);
  }
  
  delete res;
}

void
dhash::get_key_read_cb (chordID key, char *buf, unsigned int *read, 
			dhash_fetchiter_res *res, cbstat_t cb, clnt_stat err) 
{
  if ( (err) || (res->status != DHASH_COMPLETE)) {
    delete buf;
    (*cb)(DHASH_RPCERR);
  } else {
    *read += res->compl_res->res.size ();
    memcpy (buf + res->compl_res->offset, res->compl_res->res.base (), 
	    res->compl_res->res.size ());
    if (*read == res->compl_res->attr.size) {
      get_key_finish (buf, res->compl_res->res.size (), key, cb);
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
      warn << "allocating an ss for " << arg->key << "\n";
      store_state *nss = New store_state (arg->key, arg->attr.size);
      pst.insert(nss);
      ss = nss;
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

    /* statistics */
    if (ss) bytes_stored += ss->size;
    else bytes_stored += arg->data.size ();
    
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

  if (stat != 0) 
    (*cb)(DHASH_STOREERR);
  else if (type == DHASH_STORE)
    replicate_key (id, wrap (this, &dhash::store_repl_cb, cb));
  else	   
    (*cb)(DHASH_OK);
  
  store_state *ss = pst[id];
  if (ss) {
    pst.remove (pst[id]);
    delete ss;
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

void
dhash::doRPC (chordID ID, rpc_program prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb) 
{
  chordID from = host_node->my_ID ();
  host_node->chordnode->doRPC(from,
			      ID, prog, procno,
			      in, out, cb);
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
  warn << host_node->my_ID () << " " << k << " CACHED\n";
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

  printkeys ();
}

void
dhash::stop ()
{
  if (check_replica_tcb) {
    warnx << "stop replica timer\n";
    timecb_remove (check_replica_tcb);
    check_replica_tcb = NULL;
    update_replica_list ();
  }
}
