/*
 *
 * Copyright (C) 2001  Frank Dabek (fdabek@lcs.mit.edu), 
 *                     Frans Kaashoek (kaashoek@lcs.mit.edu),
 *   		       Massachusetts Institute of Technology
 * 
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

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

dhash::dhash(str dbname, vnode *node, int k, int ss, int cs, int _ss_mode) :
  key_store(ss), key_cache(cs) {

  nreplica = k;
  kc_delay = 11;
  rc_delay = 7;
  ss_mode = _ss_mode / 10;
  
  db = New dbfe();
  //set up the options we want
  dbOptions opts;
  opts.addOption("opt_async", 1);
  opts.addOption("opt_cachesize", 80000);
  opts.addOption("opt_nodesize", 4096);

  if (int err = db->opendb(const_cast < char *>(dbname.cstr()), opts)) {
    warn << "open returned: " << strerror(err) << err << "\n";
    exit (-1);
  }


  host_node = node;
  assert (host_node);

  //recursive state
  qnonce = 1;

  key_store.set_flushcb (wrap (this, &dhash::store_flush));
  key_replicate.set_flushcb (wrap (this, &dhash::store_flush));  
  key_cache.set_flushcb (wrap (this, &dhash::cache_flush));

  // pred = 0; // XXX initialize to what?
  check_replica_tcb = NULL;
  check_key_tcb = NULL;

  /* statistics */
  keys_stored = 0;
  bytes_stored = 0;
  keys_replicated = 0;
  keys_cached = 0;
  keys_served = 0;
  bytes_served = 0;
  rpc_answered = 0;

  init_key_status ();
  update_replica_list ();
  install_replica_timer ();
  install_keycheck_timer (true, host_node->my_pred ());
  transfer_initial_keys ();

  // RPC demux
  host_node->addHandler (dhash_program_1, wrap(this, &dhash::dispatch));
  delaycb (30, wrap (this, &dhash::sync_cb));
}

void
dhash::sync_cb () 
{
  warn << "sync\n";
  db->sync ();
  delaycb (30, wrap (this, &dhash::sync_cb));
}

void
dhash::dispatch (svccb *sbp) 
{

  rpc_answered++;
  switch (sbp->proc ()) {

  case DHASHPROC_FETCHITER:
    {
      warnt ("DHASH: fetchiter_request");
      
      s_dhash_fetch_arg *farg = sbp->template getarg<s_dhash_fetch_arg> ();
      
      dhash_fetchiter_res res (DHASH_CONTINUE);
      if (key_status (farg->key) != DHASH_NOTPRESENT) {
	if (farg->len > 0) {
	  //fetch the key and return it, end of story
	  fetch (farg->key, wrap (this, &dhash::fetchiter_svc_cb, sbp, farg));
	  return;
	} else {
	  // on zero length request, we just return
	  // whether we store the block or not
	  res.set_status (DHASH_COMPLETE);
	  res.compl_res->res.setsize (0);
	  res.compl_res->attr.size = 0;
	  res.compl_res->offset = 0;
	  res.compl_res->source = host_node->my_ID ();
	} 
      } else if (responsible (farg->key))  {
	//no where else to go, return NOENT or RETRY?
	res.set_status (DHASH_NOENT);
      } else {
	res.set_status (DHASH_CONTINUE);
	chordID nid;
	chordID myID = host_node->my_ID ();
	chordID my_succ = host_node->my_succ ();
	if (betweenrightincl(myID, my_succ, farg->key))
	  nid = my_succ;
	else {
	  // XXX closestpred boils down to a between() call.
	  //     don't we really want a betweenleftincl() ???
	  //     wouldn't this save one hop???
	  //
	  //     --josh
	  nid = host_node->lookup_closestpred (farg->key);
	}

	res.cont_res->next.x = nid;
	res.cont_res->next.r = 
	  host_node->chordnode->locations->getaddress (nid);

#if 0
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
	
	res.cont_res->succ_list.setsize (s_list.size ());
	for (unsigned int i = 0; i < s_list.size (); i++)
	  res.cont_res->succ_list[i] = s_list[i];

	chordID best_succ = res.cont_res->succ_list[0].x;

	res.cont_res->next.x = nid;
	res.cont_res->next.r = 
	  host_node->chordnode->locations->getaddress (best_succ);
#endif

      }
      
      sbp->reply (&res);
    }
    break;
    

  case DHASHPROC_STORE:
    {
      update_replica_list ();
      warnt("DHASH: STORE_request");

      s_dhash_insertarg *sarg = sbp->template getarg<s_dhash_insertarg> ();
     
      if ((sarg->type == DHASH_STORE) && 
	  (!responsible (sarg->key)) && 
	  (!pst[sarg->key])) {
	warnt("DHASH: retry");
	dhash_storeres res (DHASH_RETRY);
	chordID pred = host_node->my_pred ();
	res.pred->p.x = pred;
	res.pred->p.r = host_node->chordnode->locations->getaddress (pred);
	sbp->reply (&res);
      } else {
	warnt ("DHASH: will store");
	store (sarg, wrap(this, &dhash::storesvc_cb, sbp, sarg));	
      }
      
    }
    break;
  case DHASHPROC_GETKEYS:
    {
      s_dhash_getkeys_arg *gkarg = 
	sbp->template getarg<s_dhash_getkeys_arg> ();
      
      dhash_getkeys_res res (DHASH_OK);
      ref<vec<chordID> > keys = New refcounted<vec<chordID> >;
      chordID mypred = host_node->my_pred ();
      key_store.traverse (wrap (this, &dhash::get_keys_traverse_cb, keys, 
				mypred, gkarg->pred_id));

      res.resok->keys.set (keys->base (), keys->size (), freemode::NOFREE);
      sbp->reply (&res);
    }
    break;
  case DHASHPROC_KEYSTATUS:
    {
      s_dhash_keystatus_arg *arg = 
	sbp->template getarg<s_dhash_keystatus_arg> ();

      //return the status of this key (needed?)
      dhash_stat stat = key_status (arg->key);
      sbp->reply (&stat);
    }
    break;
  default:
    sbp->replyref (PROC_UNAVAIL);
    break;
  }

  pred = host_node->my_pred ();
  return;
}

void
dhash::fetchiter_svc_cb (svccb *sbp, s_dhash_fetch_arg *arg,
			 ptr<dbrec> val, dhash_stat err) 
{
  dhash_fetchiter_res res (DHASH_CONTINUE);
  if (err) 
    res.set_status (DHASH_NOENT);
  else {
    res.set_status (DHASH_COMPLETE);
    
    int n = (arg->len + arg->start < val->len) ? 
      arg->len : val->len - arg->start;

    res.compl_res->res.setsize (n);
    res.compl_res->attr.size = val->len;
    res.compl_res->offset = arg->start;
    res.compl_res->source = host_node->my_ID ();

    memcpy (res.compl_res->res.base (), 
	    (char *)val->value + arg->start, 
	    n);
    /* statistics */
    keys_served++;
    bytes_served += n;
  }
  
  sbp->reply (&res);
}

void
dhash::storesvc_cb(svccb *sbp,
		   s_dhash_insertarg *arg,
		   dhash_stat err) {
  
  warnt("DHASH: STORE_replying");

  dhash_storeres res (DHASH_OK);
  if ((err != DHASH_OK) && (err != DHASH_STORE_PARTIAL)) 
    res.set_status (err);
  else {
    res.resok->source = host_node->my_ID ();
    res.resok->done = (err == DHASH_OK);
  }

  sbp->reply (&res);
}

void
dhash::get_keys_traverse_cb (ptr<vec<chordID> > vKeys,
			     chordID mypred,
			     chordID predid,
			     const chordID &key)
{
  
  if (between (mypred, predid, key)) 
    vKeys->push_back (key);
}

//---------------- no sbp's below this line --------------
 
// -------- reliability stuff

void
dhash::init_key_status () 
{
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

    d = it->nextElement();
  } 
}

void
dhash::transfer_initial_keys ()
{
  chordID succ = host_node->my_succ ();
  if (succ ==  host_node->my_ID ()) return;

  ptr<s_dhash_getkeys_arg> arg = New refcounted<s_dhash_getkeys_arg>;
  arg->pred_id = host_node->my_ID ();
  arg->v.n = succ;

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
    if (key_status (res->resok->keys[i]) == DHASH_NOTPRESENT)
      get_key (succ, res->resok->keys[i], 
	       wrap (this, &dhash::transfer_init_gotk_cb));
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
}


void 
dhash::install_keycheck_timer (bool first, chordID pred) 
{
  check_key_tcb = delaycb (kc_delay, 0, 
			   wrap (this, &dhash::check_keys_timer_cb, first, pred));
}

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
dhash::check_replicas_traverse_cb (chordID to, const chordID &key)
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
dhash::check_keys_timer_cb (bool first, chordID pred)
{
  chordID current_pred = host_node->my_pred ();

  // only if the predecessor has changed do we need to
  // check the keys.  first handles the initial condition
  // where we don't have a previous predecessor to compare
  // against.
  if (first ||  current_pred != pred) {
    // printkeys ();
    key_store.traverse (wrap (this, &dhash::check_keys_traverse_cb));
    key_replicate.traverse (wrap (this, &dhash::check_keys_traverse_cb));
    key_cache.traverse (wrap (this, &dhash::check_keys_traverse_cb));
  }
  install_keycheck_timer (false, current_pred);
}

void
dhash::check_keys_traverse_cb (const chordID &key) 
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
      ptr<s_dhash_insertarg> i_arg = New refcounted<s_dhash_insertarg> ();
      i_arg->v.n = to;
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
			  dhash_storeres *res, ptr<s_dhash_insertarg> i_arg,
			  chordID to, clnt_stat err) 
{
  if (err) 
    cb (DHASH_RPCERR);
  else if (res->status == DHASH_RETRY) {
    dhash_storeres *nres = New dhash_storeres (DHASH_OK);
    // XXX challenge
    host_node->chordnode->locations->cacheloc (res->pred->p.x, 
					       res->pred->p.r,
					       cbchall_null);
					       
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
  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
  arg->key = key; 
  arg->len = MTU;  
  arg->start = 0;
  arg->v.n = source;

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
    memcpy (buf, res->compl_res->res.base (), res->compl_res->res.size ());
    unsigned int *read = New unsigned int(res->compl_res->res.size ());
    unsigned int offset = *read;
    do {
      ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
      arg->v.n = source;
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
    // XXX caller of get_key can get one error cb for each chunk!!!
    (*cb) (DHASH_RPCERR);
  } else {
    *read += res->compl_res->res.size ();
    memcpy (buf + res->compl_res->offset, res->compl_res->res.base (), 
	    res->compl_res->res.size ());
    if (*read == res->compl_res->attr.size) {
      get_key_finish (buf, res->compl_res->res.size (), key, cb);
      delete read;
      delete [] buf;
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
  } else
    (*cb)(ret, DHASH_OK);
}

void
dhash::append (ptr<dbrec> key, ptr<dbrec> data,
	       s_dhash_insertarg *arg,
	       cbstore cb)
{
  if (key_status (arg->key) == DHASH_NOTPRESENT) {
    //create a new record in the database
    long type = DHASH_APPEND;
    long buflen = data->len;
    xdrsuio x;
    int size = buflen + 3 & ~3;
    char *m_buf;
    if (XDR_PUTLONG (&x, (long int *)&type) &&
	XDR_PUTLONG (&x, (long int *)&buflen) &&
	(m_buf = (char *)XDR_INLINE (&x, size)))
      {
	memcpy (m_buf, data->value, buflen);
	int m_len = x.uio ()->resid ();
	char *m_dat = suio_flatten (x.uio ());
	ptr<dbrec> marshalled_data = 
	  New refcounted<dbrec> (m_dat, m_len);

	dhash_stat stat;
	chordID id = arg->key;
	stat = DHASH_STORED;
	keys_stored += key_store.enter (id, &stat);

	db->insert (key, marshalled_data, 
		    wrap(this, &dhash::append_after_db_store, cb, arg->key));

	delete m_dat;
	
      } else {
	cb (DHASH_STOREERR);
      }
  } else {
    fetch (arg->key, wrap (this, &dhash::append_after_db_fetch,
			   key, data, arg, cb));
  }
}

void
dhash::append_after_db_fetch (ptr<dbrec> key, ptr<dbrec> new_data,
			      s_dhash_insertarg *arg, cbstore cb,
			      ptr<dbrec> data, dhash_stat err)
{
  if (dhash::block_type (data) != DHASH_APPEND) {
    cb (DHASH_STORE_NOVERIFY);
  } else {
    long type = DHASH_APPEND;
    ptr<dhash_block> b = dhash::get_block_contents (data, DHASH_APPEND);
    long buflen = b->len + new_data->len;
    xdrsuio x;
    int size = buflen + 3 & ~3;
    char *m_buf;
    if (XDR_PUTLONG (&x, (long int *)&type) &&
	XDR_PUTLONG (&x, (long int *)&buflen) &&
	(m_buf = (char *)XDR_INLINE (&x, size)))
      {
	memcpy (m_buf, b->data, b->len);
	memcpy (m_buf + b->len, new_data->value, new_data->len);
	int m_len = x.uio ()->resid ();
	char *m_dat = suio_flatten (x.uio ());
	ptr<dbrec> marshalled_data = 
	  New refcounted<dbrec> (m_dat, m_len);

	db->insert (key, marshalled_data, 
		    wrap(this, &dhash::append_after_db_store, cb, arg->key));

	delete m_dat;
      } else {
	cb (DHASH_STOREERR);
      }
  }

}

void
dhash::append_after_db_store (cbstore cb, chordID k, int stat)
{
  if (stat)
    cb (DHASH_STOREERR);
  else
    cb (DHASH_OK);

  store_state *ss = pst[k];
  if (ss) {
    pst.remove (ss);
    delete ss;
  }
  //replicate?
}

void 
dhash::store (s_dhash_insertarg *arg, cbstore cb)
{
  store_state *ss = pst[arg->key];

  if (ss == NULL) {
    ss = New store_state (arg->key, arg->attr.size);
    pst.insert(ss);
  }

  if (!ss->addchunk(arg->offset, arg->offset+arg->data.size (), 
		    arg->data.base ())) {
    cb (DHASH_ERR);
    return;
  }

  if (ss->iscomplete()) {
    ptr<dbrec> k = id2dbrec(arg->key);
    ptr<dbrec> d = NULL;
    if (!ss)
      d = New refcounted<dbrec> (arg->data.base (), arg->data.size ());
    else 
      d = New refcounted<dbrec> (ss->buf, ss->size);

    if (arg->attr.ctype == DHASH_APPEND) {
      append (k, d, arg, cb);
      return;
    }
    
    if (!dhash::verify (arg->key, arg->attr.ctype, (char *)d->value, d->len) ||
	((arg->attr.ctype == DHASH_NOAUTH) 
	 && key_status (arg->key) != DHASH_NOTPRESENT)) {

      cb (DHASH_STORE_NOVERIFY);
      if (ss) {
	pst.remove (ss);
	delete ss;
      }
      return;
    }


    warnt("DHASH: STORE_before_db");
    dhash_stat stat;
    chordID id = arg->key;
    if (arg->type == DHASH_STORE) {
      stat = DHASH_STORED;
      keys_stored += key_store.enter (id, &stat);
    } else if (arg->type == DHASH_REPLICA) {
      stat = DHASH_REPLICATED;
      keys_replicated += key_replicate.enter (id, &stat);
    } else {
      stat = DHASH_CACHED;
      keys_cached += key_cache.enter (id, &stat);
    }

#if 1
    /* statistics */
    if (ss)
      bytes_stored += ss->size;
    else
      bytes_stored += arg->data.size ();
#endif
    
#if 1
    db->insert (k, d, wrap(this, &dhash::store_cb, arg->type, id, cb));
#else
    store_cb (arg->type, id, cb, 0);
#endif
  } else
    cb (DHASH_STORE_PARTIAL);
}

void
dhash::store_cb(store_status type, chordID id, cbstore cb, int stat) 
{
  warnt("DHASH: STORE_after_db");

  if (stat != 0) 
    (*cb)(DHASH_STOREERR);
  else	   
    (*cb)(DHASH_OK);

  if (type == DHASH_STORE)
    replicate_key (id, wrap (this, &dhash::store_repl_cb, cb));

  store_state *ss = pst[id];
  if (ss) {
    pst.remove (ss);
    delete ss;
  }
}

void
dhash::store_repl_cb (cbstore cb, dhash_stat err) 
{
  /*  if (err) cb (err);
      else cb (DHASH_OK); */
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
  else if (key_status (key) == DHASH_CACHED)
    key_cache.remove (key);
  else
    assert (0);

  if (newstat == DHASH_STORED)
    key_store.enter (key, &newstat);
  if (newstat == DHASH_REPLICATED)
    key_replicate.enter (key, &newstat);
  else if (newstat == DHASH_CACHED)
    key_cache.enter (key, &newstat);
}


dhash_stat
dhash::key_status(const chordID &n) 
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
dhash::responsible(const chordID& n) 
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


void
dhash::doRPC (chordID ID, rpc_program prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb) 
{
#ifdef PNODE
  host_node->doRPC (ID, prog, procno, in, out, cb);
#else /* PNODE */  
  host_node->chordnode->doRPC(ID, prog, procno,
			      in, out, cb);
#endif /* PNODE */  
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
dhash::printkeys_walk (const chordID &k) 
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
dhash::printcached_walk (const chordID &k) 
{
  warn << host_node->my_ID () << " " << k << " CACHED\n";
}

void
dhash::print_stats () 
{
  warnx << "ID: " << host_node->my_ID () << "\n";
  warnx << "Stats:\n";
  warnx << "  " << keys_stored << " keys stored\n";
  warnx << "  " << keys_cached << " keys cached\n";
  warnx << "  " << keys_replicated << " keys replicated\n";
  warnx << "  " << bytes_stored << " total bytes held\n";
  warnx << "  " << keys_served << " keys served\n";
  warnx << "  " << bytes_served << " bytes served\n";
  warnx << "  " << rpc_answered << " rpc answered\n";

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
